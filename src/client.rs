use build_proto::google::devtools::build::v1::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest, publish_build_event_client::PublishBuildEventClient,
};
use tonic::transport::Channel;

/// A trait that mimics method in the generated prost client foir the build proto.
/// Used to add a layer between the client and it's usage for mocking.
#[tonic::async_trait]
pub trait Client {
    async fn publish_build_tool_event_stream(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = PublishBuildToolEventStreamRequest> + Send,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<PublishBuildToolEventStreamResponse>>,
        tonic::Status,
    >;

    async fn publish_lifecycle_event(
        &mut self,
        request: impl tonic::IntoRequest<PublishLifecycleEventRequest> + Send,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
}

#[tonic::async_trait]
impl Client for PublishBuildEventClient<Channel> {
    async fn publish_build_tool_event_stream(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = PublishBuildToolEventStreamRequest> + Send,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<PublishBuildToolEventStreamResponse>>,
        tonic::Status,
    > {
        PublishBuildEventClient::<Channel>::publish_build_tool_event_stream(self, request).await
    }

    async fn publish_lifecycle_event(
        &mut self,
        request: impl tonic::IntoRequest<PublishLifecycleEventRequest> + Send,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        PublishBuildEventClient::<Channel>::publish_lifecycle_event(self, request).await
    }
}
