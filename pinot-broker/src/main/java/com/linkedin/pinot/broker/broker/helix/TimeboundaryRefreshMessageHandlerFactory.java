package com.linkedin.pinot.broker.broker.helix;

import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.common.messages.TimeboundaryRefreshMessage;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Handle the TimeboundaryRefresh message.
public class TimeboundaryRefreshMessageHandlerFactory implements MessageHandlerFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeboundaryRefreshMessageHandlerFactory.class);
    private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
    public TimeboundaryRefreshMessageHandlerFactory(HelixExternalViewBasedRouting helixExternalViewBasedRouting) {
        _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
        String msgSubType = message.getMsgSubType();
        switch (msgSubType) {
            case TimeboundaryRefreshMessage.REFRESH_TIME_BOUNDARY_MSG_SUB_TYPE:
                LOGGER.warn("time refresh msg received");
                return new TimeboundaryRefreshMessageHandler(new TimeboundaryRefreshMessage(message), context);
            default:
                throw new UnsupportedOperationException("Unsupported user defined message sub type: " + msgSubType);
        }
    }

    @Override
    public String getMessageType() {
        return Message.MessageType.USER_DEFINE_MSG.toString();
    }

    @Override
    public void reset() {
        LOGGER.info("Reset called");
    }

    private class TimeboundaryRefreshMessageHandler extends MessageHandler{
        private final String _tableNameWithType;
        private final Logger _logger;


        public TimeboundaryRefreshMessageHandler(TimeboundaryRefreshMessage message, NotificationContext context) {
            super(message, context);
            _tableNameWithType = (message.getResourceName() != null && message.getResourceName().length() > 0) ?
                    message.getResourceName() :
                    message.getRecord().getSimpleField("TableName");
            _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + TimeboundaryRefreshMessageHandler.class);
        }

        @Override
        public HelixTaskResult handleMessage() throws InterruptedException {
            // Update the timeboundary of the pinot table after receiving the segment refresh message.
            HelixTaskResult result = new HelixTaskResult();
            _logger.info("Handling message: {}", _message);
            try {
                _helixExternalViewBasedRouting.updateTimeboundaryForTable(_tableNameWithType);
                result.setSuccess(true);
            } finally {
            }
            return result;
        }

        @Override
        public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
            _logger.error("onError: {}, {}", errorType, errorCode, e);
        }
    }
}
