pub mod counter {
    pub mod v1 {
        tonic::include_proto!("counter.v1");
    }
}

pub mod replication {
    pub mod v1 {
        tonic::include_proto!("replication.v1");
    }
}
