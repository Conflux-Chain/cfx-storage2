use error_chain::error_chain;

error_chain! {
    links {
    }

    foreign_links {
        File(std::io::Error);
        Serialize(ark_serialize::SerializationError);
    }

    errors {
        InconsistentLength {
            description("In consistent length between expected params and real params")
            display("In consistent length between expected params and real params")
        }

        InconsistentPowersOfTau {
            description("In consistent powers of tau")
            display("In consistent powers of tau")
        }

        RareZeroGenerationError {
            description("Failed to generate a non-zero scalar after multiple attempts")
            display("Failed to generate a non-zero scalar after multiple attempts")
        }
    }
}
