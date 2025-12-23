use a2db_core::{CounterStore, Delta};
use a2db_proto::counter::v1::{
    counter_service_server::CounterService, GetRequest, GetResponse, IncrByRequest,
    IncrByResponse, MGetRequest, MGetResponse,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{debug, instrument};

/// Implementation of the client-facing CounterService gRPC service
pub struct CounterServiceImpl {
    /// The underlying counter store
    store: Arc<CounterStore>,
    /// Channel to send deltas for replication
    delta_tx: mpsc::Sender<Delta>,
}

impl CounterServiceImpl {
    pub fn new(store: Arc<CounterStore>, delta_tx: mpsc::Sender<Delta>) -> Self {
        Self { store, delta_tx }
    }
}

#[tonic::async_trait]
impl CounterService for CounterServiceImpl {
    #[instrument(skip(self, request), fields(key = %request.get_ref().key, amount = %request.get_ref().amount))]
    async fn incr_by(
        &self,
        request: Request<IncrByRequest>,
    ) -> Result<Response<IncrByResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        if req.amount == 0 {
            // Increment by 0 is a no-op, just return current value
            let value = self.store.get(&req.key);
            return Ok(Response::new(IncrByResponse { value }));
        }

        match self.store.increment_str(&req.key, req.amount) {
            Some((value, delta)) => {
                // Send delta for replication (non-blocking)
                // If the channel is full, we log a warning but don't fail the request
                // The delta will be part of the next anti-entropy sync
                if let Err(e) = self.delta_tx.try_send(delta) {
                    tracing::warn!("Failed to queue delta for replication: {}", e);
                }

                debug!(value = value, "Incremented counter");

                Ok(Response::new(IncrByResponse { value }))
            }
            None => {
                Err(Status::failed_precondition("WRONGTYPE Operation against a key holding the wrong kind of value"))
            }
        }
    }

    #[instrument(skip(self, request), fields(key = %request.get_ref().key))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        let value = self.store.get(&req.key);

        debug!(value = value, "Retrieved counter value");

        Ok(Response::new(GetResponse { value }))
    }

    #[instrument(skip(self, request), fields(key_count = %request.get_ref().keys.len()))]
    async fn m_get(&self, request: Request<MGetRequest>) -> Result<Response<MGetResponse>, Status> {
        let req = request.into_inner();

        if req.keys.is_empty() {
            return Err(Status::invalid_argument("keys cannot be empty"));
        }

        // Validate all keys
        for (i, key) in req.keys.iter().enumerate() {
            if key.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "key at index {} cannot be empty",
                    i
                )));
            }
        }

        let values = self.store.mget(&req.keys);

        debug!(values = ?values, "Retrieved multiple counter values");

        Ok(Response::new(MGetResponse { values }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn create_test_service() -> (CounterServiceImpl, mpsc::Receiver<Delta>) {
        let store = Arc::new(CounterStore::from_str("test-replica"));
        let (tx, rx) = mpsc::channel(100);
        (CounterServiceImpl::new(store, tx), rx)
    }

    #[tokio::test]
    async fn test_incr_by() {
        let (service, mut rx) = create_test_service();

        let request = Request::new(IncrByRequest {
            key: "test:counter".into(),
            amount: 5,
        });

        let response = service.incr_by(request).await.unwrap();
        assert_eq!(response.into_inner().value, 5);

        // Verify delta was sent
        let delta = rx.try_recv().unwrap();
        assert_eq!(delta.key.as_ref(), "test:counter");
        assert_eq!(delta.component_value, 5);
    }

    #[tokio::test]
    async fn test_incr_by_zero() {
        let (service, mut rx) = create_test_service();

        // First increment normally
        let request = Request::new(IncrByRequest {
            key: "test:counter".into(),
            amount: 10,
        });
        service.incr_by(request).await.unwrap();
        rx.try_recv().unwrap(); // consume delta

        // Then increment by 0
        let request = Request::new(IncrByRequest {
            key: "test:counter".into(),
            amount: 0,
        });
        let response = service.incr_by(request).await.unwrap();
        assert_eq!(response.into_inner().value, 10);

        // No delta should be sent for 0 increment
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_incr_by_empty_key() {
        let (service, _rx) = create_test_service();

        let request = Request::new(IncrByRequest {
            key: "".into(),
            amount: 5,
        });

        let result = service.incr_by(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get() {
        let (service, _rx) = create_test_service();

        // Get non-existent key
        let request = Request::new(GetRequest {
            key: "test:counter".into(),
        });
        let response = service.get(request).await.unwrap();
        assert_eq!(response.into_inner().value, 0);

        // Increment and get
        let request = Request::new(IncrByRequest {
            key: "test:counter".into(),
            amount: 42,
        });
        service.incr_by(request).await.unwrap();

        let request = Request::new(GetRequest {
            key: "test:counter".into(),
        });
        let response = service.get(request).await.unwrap();
        assert_eq!(response.into_inner().value, 42);
    }

    #[tokio::test]
    async fn test_get_empty_key() {
        let (service, _rx) = create_test_service();

        let request = Request::new(GetRequest { key: "".into() });
        let result = service.get(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mget() {
        let (service, _rx) = create_test_service();

        // Set up some counters
        service
            .incr_by(Request::new(IncrByRequest {
                key: "k1".into(),
                amount: 10,
            }))
            .await
            .unwrap();
        service
            .incr_by(Request::new(IncrByRequest {
                key: "k2".into(),
                amount: 20,
            }))
            .await
            .unwrap();

        let request = Request::new(MGetRequest {
            keys: vec!["k1".into(), "k3".into(), "k2".into()],
        });
        let response = service.m_get(request).await.unwrap();
        assert_eq!(response.into_inner().values, vec![10, 0, 20]);
    }

    #[tokio::test]
    async fn test_mget_empty_keys() {
        let (service, _rx) = create_test_service();

        let request = Request::new(MGetRequest { keys: vec![] });
        let result = service.m_get(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mget_with_empty_key_in_list() {
        let (service, _rx) = create_test_service();

        let request = Request::new(MGetRequest {
            keys: vec!["k1".into(), "".into(), "k2".into()],
        });
        let result = service.m_get(request).await;
        assert!(result.is_err());
    }
}
