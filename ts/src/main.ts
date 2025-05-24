import { SuiClient } from '@mysten/sui/client'
import { Command } from 'commander'
import { HistoricalDevInspectClient } from './client'
import { Transaction } from '@mysten/sui/transactions'
import { toBase64 } from '@mysten/sui/utils'
import { bcs } from '@mysten/sui/bcs'

const program = new Command()

const client = new SuiClient({
  url: 'https://fullnode.mainnet.sui.io:443',
})

const HISTORICAL_DEV_INSPECT_BASE_URL = 'http://localhost:8000'

export const clockTimestampCommand = (client: SuiClient) => {
  return new Command('clock-timestamp')
    .argument('<checkpointSequenceNumber>', 'checkpoint sequence number')
    .action(async (checkpointSequenceNumber: string) => {
      const histClient = new HistoricalDevInspectClient(
        HISTORICAL_DEV_INSPECT_BASE_URL,
        client
      )

      const tx = new Transaction()

      tx.moveCall({
        target: '0x2::clock::timestamp_ms',
        arguments: [tx.object.clock()],
      })

      const txBytes = await tx.build({ client, onlyTransactionKind: true })

      const txData = toBase64(txBytes)

      const res = await histClient.devInspectTransactionBlock({
        sender:
          '0xc6cbb76cf1ef5091296bd64be1d522fce501e89ff492899334b55d8efb4769a6',
        transactionBlock: txData,
        executionPoint: {
          checkpointSequenceNumber: Number(checkpointSequenceNumber),
          position: 'after',
        },
      })

      const ts = Number(
        bcs
          .u64()
          .parse(
            Uint8Array.from(res.results?.[0].returnValues?.[0][0] as number[])
          )
      )
      console.log(new Date(ts))
    })
}

program.addCommand(clockTimestampCommand(client))

program.parse(process.argv)
