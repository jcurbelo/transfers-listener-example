import dotenv from 'dotenv';
import BeeQueue from 'bee-queue';
import { ethers } from 'ethers';
import Redis from 'ioredis';

type ERC721Transfer = {
  txHash: string;
  from: string;
  to: string;
  tokenId: number;
};

class RedisClient extends Redis {
  constructor(redisUrl: string) {
    super(redisUrl, {
      enableAutoPipelining: true,
    });
  }
}

dotenv.config();

const contract = new ethers.Contract(
  process.env.CONTRACT || '',
  [
    {
      anonymous: false,
      inputs: [
        {
          indexed: true,
          internalType: 'address',
          name: 'from',
          type: 'address',
        },
        {
          indexed: true,
          internalType: 'address',
          name: 'to',
          type: 'address',
        },
        {
          indexed: true,
          internalType: 'uint256',
          name: 'tokenId',
          type: 'uint256',
        },
      ],
      name: 'Transfer',
      type: 'event',
    },
  ],
  new ethers.providers.AlchemyWebSocketProvider(
    1, // mainnet
    process.env.ALCHEMY_KEY || ''
  )
);

const transferEvent = contract.filters.Transfer();

const config = {
  redis: new RedisClient(process.env.REDIS_URL || ''),
};

const transfersQueue = new BeeQueue<ERC721Transfer>('transfers', config);

transfersQueue.process(
  async (
    job: BeeQueue.Job<ERC721Transfer>,
    done: BeeQueue.DoneCallback<unknown>
  ) => {
    const event = job.data;
    console.log(`[worker] processing event: ${JSON.stringify(event)}.`);
    done(null, job.data);
  }
);

const enqueueJob = (
  txHash: string,
  from: string,
  to: string,
  tokenId: number,
  source: 'listener' | 'bootstrap'
) => {
  const event: ERC721Transfer = {
    txHash: txHash.toLowerCase(),
    from: from.toLowerCase(),
    to: to.toLowerCase(),
    tokenId,
  };
  transfersQueue
    .createJob<ERC721Transfer>(event)
    .setId(txHash.toLowerCase())
    .retries(7)
    .save(() => {
      console.log(`[${source}] enqueue transfer for hash ${txHash}.`);
    });
};

const bootstrap = async () => {
  console.log('[bootstrap] starting up.');
  console.log('[bootstrap] fetching transfers.');

  const events = await contract.queryFilter(transferEvent);
  console.log(`[bootstrap] found ${events.length} transfers.`);

  events.forEach((event) => {
    enqueueJob(
      event.transactionHash,
      event.args?.from,
      event.args?.to,
      Number(event.args?.tokenId),
      'bootstrap'
    );
  });

  console.log('[bootstrap] complete.');
};

(async () => {
  await bootstrap();

  contract.on(transferEvent, (from, to, tokenId, event) => {
    enqueueJob(event.transactionHash, from, to, Number(tokenId), 'listener');
  });
})();
