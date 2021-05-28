#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteRequest {
    #[prost(string, tag = "1")]
    pub sql: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteResponse {
    #[prost(oneof = "execute_response::Item", tags = "1, 2, 3")]
    pub item: ::core::option::Option<execute_response::Item>,
}
/// Nested message and enum types in `ExecuteResponse`.
pub mod execute_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DataSet {
        #[prost(bytes = "vec", tag = "1")]
        pub dataset: ::prost::alloc::vec::Vec<u8>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Metrics {
        #[prost(int64, tag = "1")]
        pub start_time: i64,
        #[prost(int64, tag = "2")]
        pub end_time: i64,
        #[prost(int64, tag = "3")]
        pub num_input_rows: i64,
        #[prost(int64, tag = "4")]
        pub num_output_rows: i64,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Error {
        #[prost(string, tag = "1")]
        pub error: ::prost::alloc::string::String,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Item {
        #[prost(message, tag = "1")]
        Dataset(DataSet),
        #[prost(message, tag = "2")]
        Metrics(Metrics),
        #[prost(message, tag = "3")]
        Error(Error),
    }
}
#[doc = r" Generated client implementations."]
pub mod yql_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct YqlClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl YqlClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> YqlClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn execute(
            &mut self,
            request: impl tonic::IntoRequest<super::ExecuteRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::ExecuteResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/yql.Yql/Execute");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for YqlClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for YqlClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "YqlClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod yql_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with YqlServer."]
    #[async_trait]
    pub trait Yql: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Execute method."]
        type ExecuteStream: futures_core::Stream<Item = Result<super::ExecuteResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn execute(
            &self,
            request: tonic::Request<super::ExecuteRequest>,
        ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct YqlServer<T: Yql> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Yql> YqlServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for YqlServer<T>
    where
        T: Yql,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/yql.Yql/Execute" => {
                    #[allow(non_camel_case_types)]
                    struct ExecuteSvc<T: Yql>(pub Arc<T>);
                    impl<T: Yql> tonic::server::ServerStreamingService<super::ExecuteRequest> for ExecuteSvc<T> {
                        type Response = super::ExecuteResponse;
                        type ResponseStream = T::ExecuteStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExecuteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).execute(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = ExecuteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Yql> Clone for YqlServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Yql> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Yql> tonic::transport::NamedService for YqlServer<T> {
        const NAME: &'static str = "yql.Yql";
    }
}
