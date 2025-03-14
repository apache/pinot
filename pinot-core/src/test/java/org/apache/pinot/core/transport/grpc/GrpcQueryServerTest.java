package org.apache.pinot.core.transport.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.trace.Tracing;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class GrpcQueryServerTest {
    private GrpcQueryServer _grpcQueryServer;
    @Mock private QueryExecutor _queryExecutor;
    @Mock private ServerMetrics _serverMetrics;
    @Mock private AccessControl _accessControl;
    @Mock private StreamObserver<Server.ServerResponse> _responseObserver;
    @Mock private GrpcConfig _grpcConfig;

    @BeforeMethod
    public void setUp() {
        _queryExecutor = mock(QueryExecutor.class);
        _serverMetrics = mock(ServerMetrics.class);
        _accessControl = mock(AccessControl.class);
        _responseObserver = mock(StreamObserver.class);
        _grpcConfig = mock(GrpcConfig.class);

        _grpcQueryServer = new GrpcQueryServer(1234, _grpcConfig, null,
                _queryExecutor, _serverMetrics, _accessControl);
        _grpcQueryServer.start();
    }

    @Test
    public void testEnsureGrpcQueryProcessingInitializesTracingContext() {
        // Arrange
        InstanceResponseBlock mockedInstanceResponseBlock = mock(InstanceResponseBlock.class);
        doNothing().when(_serverMetrics).addMeteredGlobalValue(any(), anyLong());
        when(_accessControl.hasDataAccess(any(), any())).thenReturn(false);
        when(_queryExecutor.execute(any(), any(), any())).thenReturn(mockedInstanceResponseBlock);

        Server.ServerRequest serverRequest = Server.ServerRequest
                .newBuilder()
                .setSql("SELECT x FROM myTable")
                .build();

        try (MockedStatic<Tracing.ThreadAccountantOps> mockedStatic =
                     mockStatic(Tracing.ThreadAccountantOps.class)) {
            // Act
            _grpcQueryServer.submit(serverRequest, _responseObserver);

            // Assert: ThreadAccountantOps.setupRunner() was called once.
            mockedStatic.verify(() -> Tracing.ThreadAccountantOps.setupRunner(anyString()), times(1));
        }
    }

    @AfterMethod
    public void tearDown() {
        _grpcQueryServer.shutdown();
    }
}
