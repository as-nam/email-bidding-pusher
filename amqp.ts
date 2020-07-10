import amqp, {Channel} from "amqplib";

export let rabbitConnection = null;
const url = 'https://api.emailbidding.com/api/p/publishers/172/lists/de_eb_as/recipients-batch?key=5d07251f58ae4e2cc6578d6333ede432&secret=77b9fa8e8211b4613619a015c81b984c8e52e220';

export const initRabbitMQ = async () => {
    try {
        const conn = await amqp.connect('amqp://localhost');
        console.log(`[AMQP] CONNECTED`);
        conn.on("error", (e) => {
            if (e.message !== "Connection closing") {
                console.error("[AMQP] conn error from publisher", e.message);
            }
        });
        conn.on("close", function () {
            console.error("[AMQP] reconnecting from publisher");
            setTimeout(initRabbitMQ, 3000);
        });
        rabbitConnection = conn;
    } catch (e) {
        console.error("Error connection to rabbit", e.stack);
        setTimeout(initRabbitMQ, 3000);
    }
};

export const uploadFilePublisher = async () => {
    try {
        const channel: Channel = await rabbitConnection.createChannel();
        const msg: string = "Hello World!";
        await channel.assertQueue("UPLOAD_FILE", {
            durable: true,
        });
        await channel.sendToQueue("UPLOAD_FILE", Buffer.from(msg), {
            persistent: true,
        });
    } catch (e) {
        console.error("Error create chanel to ", "UPLOAD_FILE", ": ", e);
        rabbitConnection.close();
    }
}

export const uploadFileConsumer = async () => {
    try {
        const channel = await rabbitConnection.createChannel();
        await channel.assertQueue("UPLOAD_FILE");
        console.log(" [*] Waiting for messages. To exit press CTRL+C");
        await channel.prefetch(1);
        await channel.consume("UPLOAD_FILE", handleMessage, {noAck: false});

        function handleMessage(msg) {
            const body = msg.content.toString();
            console.log(" [x] Received '%s'", body);
            //console.log(" [x] Task takes %d seconds", secs);
            setTimeout(function () {
                console.log(" [x] Done");
                channel.ack(msg);
            }, 1000);
        }
    } catch (e) {
        console.error(`Error receive data from ${"UPLOAD_FILE"}: ${e}`);
        rabbitConnection.close();
    }
}
