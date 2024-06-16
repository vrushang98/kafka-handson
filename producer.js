const { kafka } = require("./client");

const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();

  console.log("Connecting producer");

  await producer.connect();

  console.log("Producer connected successfully");

  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async (line) => {
    const [driverName, location] = line.split(" ");

    await producer.send({
      topic: "driver-updates",
      messages: [
        {
          partition: location.toLowerCase() === "north" ? 0 : 1,
          key: "location-update",
          value: JSON.stringify({ name: driverName, location: location }),
        },
      ],
    });
  }).on("close", async () => await producer.disconnect());
}

init();
