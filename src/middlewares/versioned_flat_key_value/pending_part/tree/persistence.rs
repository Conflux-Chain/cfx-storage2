use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

// --- 日志记录的数据结构 ---
// 这个结构体代表了需要持久化到日志中的单条记录。
// 它必须是可序列化和反序列化的。
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct LogRecord<C: PartialEq> {
    pub parent_of_root: Option<C>,
    pub height_of_root: u64,
}

impl<C: PartialEq> LogRecord<C> {
    pub fn check_consistency_with_db(
        &self,
        parent_of_root: Option<C>,
        height_of_root: u64,
    ) -> bool {
        (self.parent_of_root == parent_of_root) && (self.height_of_root == height_of_root)
    }
}

// --- 日志管理器 ---
#[derive(Debug)]
pub struct TreeLogger {
    log_path: PathBuf,
}

impl TreeLogger {
    /// 创建一个新的 TreeLogger 实例。
    ///
    /// # Arguments
    /// * `path` - 日志文件的路径。
    pub fn new(path: impl AsRef<Path>) -> Self {
        TreeLogger {
            log_path: path.as_ref().to_path_buf(),
        }
    }

    /// 将一次 root 变更写入日志。
    ///
    /// 这是实现 WAL 的核心。每次写入都以追加模式打开文件，
    /// 写入数据，然后写入数据长度，最后确保数据被同步到磁盘。
    ///
    /// # Arguments
    /// * `parent_of_root` - 新的 parent_of_root 值。
    /// * `height_of_root` - 新的 height_of_root 值。
    pub fn log_change<C>(&self, parent_of_root: &Option<C>, height_of_root: u64) -> io::Result<()>
    where
        C: Clone + Serialize + PartialEq,
    {
        let record = LogRecord {
            parent_of_root: parent_of_root.clone(),
            height_of_root,
        };

        // 1. 序列化记录
        let serialized_data =
            bincode::serialize(&record).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let data_len = serialized_data.len() as u64;

        // 2. 以追加模式打开文件
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        // 3. 写入序列化后的数据
        file.write_all(&serialized_data)?;

        // 4. 写入数据长度 (u64, 8 bytes)
        file.write_all(&data_len.to_le_bytes())?;

        // 5. 强制将数据同步到磁盘，确保持久化
        file.sync_all()?;

        Ok(())
    }

    /// 返回一个从后向前读取日志的迭代器。
    ///
    /// 这对于恢复过程至关重要，因为你需要从最近的、可能有效的状态开始检查。
    pub fn read_rev<C>(&self) -> io::Result<LogRevIterator<C>>
    where
        C: DeserializeOwned,
    {
        let file = File::open(&self.log_path)?;
        LogRevIterator::new(file)
    }
}

// --- 反向读取日志的迭代器 ---
pub struct LogRevIterator<C> {
    file: File,
    // 当前在文件中的位置，从文件末尾开始向前移动
    current_pos: u64,
    _phantom: PhantomData<C>,
}

impl<C> LogRevIterator<C>
where
    C: DeserializeOwned,
{
    /// 创建一个新的反向迭代器。
    fn new(mut file: File) -> io::Result<Self> {
        // 获取文件总长度，作为迭代的起始位置
        let end_pos = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            current_pos: end_pos,
            _phantom: PhantomData,
        })
    }
}

impl<C: PartialEq> Iterator for LogRevIterator<C>
where
    C: DeserializeOwned,
{
    type Item = io::Result<LogRecord<C>>;

    fn next(&mut self) -> Option<Self::Item> {
        // 如果已经读到文件开头，则停止迭代
        if self.current_pos == 0 {
            return None;
        }

        // 检查文件位置是否足够读取一个 u64 长度
        if self.current_pos < 8 {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Log file format error: insufficient space for length.",
            )));
        }

        // 移动到记录长度信息的位置
        // self.current_pos 指向 [data][length] 中 length 的末尾
        // 我们需要先读 length
        let length_pos = self.current_pos - 8;
        if let Err(e) = self.file.seek(SeekFrom::Start(length_pos)) {
            return Some(Err(e));
        }

        // 读取 8 字节的长度信息
        let mut len_buf = [0u8; 8];
        if let Err(e) = self.file.read_exact(&mut len_buf) {
            return Some(Err(e));
        }
        let data_len = u64::from_le_bytes(len_buf);

        // 计算数据块的起始位置
        if length_pos < data_len {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Log file format error: record length exceeds available data.",
            )));
        }
        let data_pos = length_pos - data_len;

        // 移动到数据块的起始位置
        if let Err(e) = self.file.seek(SeekFrom::Start(data_pos)) {
            return Some(Err(e));
        }

        // 读取实际的数据
        let mut data_buf = vec![0; data_len as usize];
        if let Err(e) = self.file.read_exact(&mut data_buf) {
            return Some(Err(e));
        }

        // 更新迭代器的位置，指向当前记录的开头，为下一次迭代做准备
        self.current_pos = data_pos;

        // 反序列化数据
        match bincode::deserialize(&data_buf) {
            Ok(record) => Some(Ok(record)),
            Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
        }
    }
}
