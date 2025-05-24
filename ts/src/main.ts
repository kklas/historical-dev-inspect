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

const histClient = new HistoricalDevInspectClient(
  'http://localhost:8000',
  client
)

export const clockTimestampCommand = () => {
  return new Command('clock-timestamp')
    .argument('<checkpointSequenceNumber>', 'checkpoint sequence number')
    .action(async (checkpointSequenceNumber: string) => {
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

function printTxCommands(tx: Transaction) {
  tx.getData().commands.forEach((command, i) => {
    if ('MoveCall' in command) {
      const { package: pkg, module, function: fn } = command.MoveCall!
      console.log(i, `MoveCall ${pkg}::${module}::${fn}`)
    } else {
      console.log(i, command.$kind)
    }
  })
}

export const cetusSwapCommand = () => {
  return new Command('cetus-swap')
    .argument('<checkpointSequenceNumber>', 'checkpoint sequence number')
    .option('-v, --verbose', 'verbose output')
    .action(
      async (
        checkpointSequenceNumber: string,
        options: { verbose: boolean }
      ) => {
        const tx = new Transaction()

        const usdcTypeArg =
          '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC'
        const suiTypeArg = '0x2::sui::SUI'

        const poolId =
          '0xb8d7d9e66a60c239e7a60110efcf8de6c705580ed924d0dde141f4a0e2c90105' // USDC - SUI
        const cetusGlobalConfigId =
          '0xdaa46292632c3c4d8f31f23ea0f9b36a28ff3677e9684980e4438403a67a3d8f'
        const a2b = false
        const byAmountIn = true
        const amount = 1_000_000_000n // 1 SUI
        const sqrtPriceLimit = 79226673515401279992447579055n

        const cetusPackageId =
          '0xc6faf3703b0e8ba9ed06b7851134bbbe7565eb35ff823fd78432baa4cbeaa12e'

        const [outA, outB, receipt] = tx.moveCall({
          target: `${cetusPackageId}::pool::flash_swap`,
          typeArguments: [usdcTypeArg, suiTypeArg],
          arguments: [
            tx.object(cetusGlobalConfigId),
            tx.object(poolId),
            tx.pure.bool(a2b),
            tx.pure.bool(byAmountIn),
            tx.pure.u64(amount),
            tx.pure.u128(sqrtPriceLimit),
            tx.object.clock(),
          ],
        })

        tx.moveCall({
          target: `0x2::balance::destroy_zero`,
          typeArguments: [suiTypeArg],
          arguments: [tx.object(outB)],
        })

        const suiCoinId =
          '0x573750051939560d8c8542371fda41c6a0c77d6d9b6d15c853237c2496da9b78'
        const [suiCoin] = tx.splitCoins(suiCoinId, [amount])
        const suiBalance = tx.moveCall({
          target: '0x2::coin::into_balance',
          typeArguments: [suiTypeArg],
          arguments: [tx.object(suiCoin)],
        })

        const repayA = suiBalance
        const repayB = tx.moveCall({
          target: '0x2::balance::zero',
          typeArguments: [usdcTypeArg],
        })

        tx.moveCall({
          target: `${cetusPackageId}::pool::repay_flash_swap`,
          typeArguments: [usdcTypeArg, suiTypeArg],
          arguments: [
            tx.object(cetusGlobalConfigId),
            tx.object(poolId),
            tx.object(repayB),
            tx.object(repayA),
            tx.object(receipt),
          ],
        })

        const outCoin = tx.moveCall({
          target: '0x2::coin::from_balance',
          typeArguments: [usdcTypeArg],
          arguments: [tx.object(outA)],
        })

        tx.moveCall({
          target: '0x2::coin::value',
          typeArguments: [usdcTypeArg],
          arguments: [tx.object(outCoin)],
        })

        const sender =
          '0xb2e4292e1cacd7fd49c9207b0209179fca189ac38b0251a59bf4e840226deb86'
        tx.transferObjects([tx.object(outCoin)], sender)

        if (options.verbose) {
          printTxCommands(tx)
        }

        const res = await histClient.devInspectTransactionBlock({
          sender,
          transactionBlock: tx,
          executionPoint: {
            checkpointSequenceNumber: Number(checkpointSequenceNumber),
            position: 'after',
          },
        })

        if (options.verbose) {
          console.log(res)
          console.log()
        }

        console.log(
          'USDC out:',
          Number(
            bcs
              .u64()
              .parse(
                Uint8Array.from(
                  res.results?.[7].returnValues?.[0][0] as number[]
                )
              )
          ) / 1_000_000
        )
      }
    )
}

program.addCommand(clockTimestampCommand())
program.addCommand(cetusSwapCommand())

program.parse(process.argv)
