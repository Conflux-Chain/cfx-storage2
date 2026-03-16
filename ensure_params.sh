#!/bin/bash

DIR_PATH="pp"
FILE_NAME="amt-prove-mont-nxssWC-16-16.bin"
FILE_PATH="${DIR_PATH}/${FILE_NAME}"
DOWNLOAD_URL="https://conflux-rust-dev.s3.ap-east-1.amazonaws.com/amt-params/amt-prove-mont-nxssWC-16-16.bin"

# Check if the file exists
if [ ! -f "$FILE_PATH" ]; then
    echo "[-] Dependency file not found: $FILE_PATH"
    echo "[-] Downloading from remote..."
    
    # Ensure the target directory exists
    mkdir -p "$DIR_PATH"
    
    # Download the file using wget
    wget -P "$DIR_PATH/" "$DOWNLOAD_URL"
    
    # Check if the wget command was successful
    if [ $? -ne 0 ]; then
        echo "[x] Download failed. Please check your network connection or the URL."
        exit 1
    fi
    echo "[+] Download completed successfully!"
else
    echo "[+] Dependency file already exists: $FILE_PATH. Skipping download."
fi