package com.victor.ifpb.rabbitmq;

import com.rabbitmq.client.*;

import java.util.Arrays;

public class RpcServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";
    private static final String FULL_NAME = " Victor Eduardo Japyassu Nobrega";
    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {
        //Criacao de uma factory de conexao, responsavel por criar as conexões.
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("mqadmin");
        connectionFactory.setPassword("Admin123XX_");
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME);

        channel.basicQos(1); // número máximo de msg que o servidor pode enviar ao consumidor antes de esperar uma confirmação.

        System.out.println(" [x] Awaiting RPC requests");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                int n = Integer.parseInt(message.split(" ")[0]);

                System.out.println(" [.] fib(" + n + ")");
                System.out.println(" [.] Aluno: " + message.substring(2));
                response += fib(n);
                response += FULL_NAME;
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e);
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));
    }

}
