const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
  const consumer = kafka.consumer({ groupId: group });

  await consumer.subscribe({ topics: ["driver-updates"], fromBeginning: true });

  await consumer.run({
    eachMessage: ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `Group: ${group} [${topic}]: PART:${partition}:`,
        message.value.toString()
      );
    },
  });

  //   await consumer.disconnect();
}

init();
