const { kafka } = require("./client");

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting...");
  admin.connect();
  console.log("Admin connected successfully...");

  console.log("Creating Topic...");
  await admin.createTopics({
    topics: [{ topic: "driver-updates", numPartitions: 2 }],
  });

  console.log("Topic Created successfully...");

  console.log("Disconnected admin...");
  await admin.disconnect();
}

init();
