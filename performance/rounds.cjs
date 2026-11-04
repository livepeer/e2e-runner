// Keep the short disposable protocol rounds initialized. This setup process
// talks directly to the chain, outside both measured worker proxies.
const {ethers} = require("ethers");
const fs = require("fs");
const deployment = JSON.parse(fs.readFileSync("/work/sources/protocol/deployments/gethDev/RoundsManager.json"));
const provider = new ethers.providers.JsonRpcProvider("http://127.0.0.1:8545");
const rounds = new ethers.Contract(deployment.address, ["function currentRoundInitialized() view returns (bool)", "function initializeRound()"], provider.getSigner(0));
(async () => {
  // A two-round devnet unlock avoids a 2,500-second withdrawal wait. This is
  // identical for every candidate and does not change payment-path code.
  const brokerDeployment = JSON.parse(fs.readFileSync("/work/sources/protocol/deployments/gethDev/TicketBroker.json"));
  const broker = new ethers.Contract(brokerDeployment.address, ["function setUnlockPeriod(uint256)"], provider.getSigner(0));
  await (await broker.setUnlockPeriod(2, {gasLimit: 2000000})).wait();
  console.log("test protocol unlockPeriod=2");
  for (;;) {
    try {
      if (!(await rounds.currentRoundInitialized())) {
        // The next poll verifies inclusion. Waiting indefinitely for a hash
        // would strand this keeper if a test reverts its submission branch.
        await rounds.initializeRound({gasLimit: 2000000});
      }
    } catch (error) {
      console.error("round initialization:", error.reason || error.code || "failed");
    }
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
})();
