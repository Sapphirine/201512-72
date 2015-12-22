package edu.columbia.cs6893;

import edu.columbia.cs6893.handler.HttpRequestHandler;
import edu.columbia.cs6893.handler.WebSocketHandler;
import edu.columbia.cs6893.kafka.KafkaConsumerFactory;
import io.vertx.core.AbstractVerticle;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class WebServerVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        WebSocketHandler webSocketHandler = new WebSocketHandler();

        vertx.createHttpServer()
                .websocketHandler(webSocketHandler)
                .requestHandler(new HttpRequestHandler())
                .listen(8080);

        KafkaConsumerFactory.startConsumer(webSocketHandler);
    }
}