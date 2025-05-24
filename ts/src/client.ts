import { DevInspectResults, SuiClient } from '@mysten/sui/client'
import { isTransaction, Transaction } from '@mysten/sui/transactions'
import { toBase64 } from '@mysten/sui/utils'

export type ExecutionPoint =
  | {
      checkpointSequenceNumber: number
      position: 'before' | 'after'
    }
  | {
      transactionDigest: string
      position: 'before' | 'after'
    }

export interface HistoricalDevInspectTransactionBlockParams {
  sender: string
  transactionBlock: Transaction | Uint8Array | string
  executionPoint: ExecutionPoint
  gasPrice?: bigint | number | null | undefined
}

export class HistoricalDevInspectClient {
  constructor(
    private baseUrl: string,
    private suiClient: SuiClient
  ) {
    this.baseUrl = baseUrl
  }

  async devInspectTransactionBlock(
    input: HistoricalDevInspectTransactionBlockParams
  ): Promise<DevInspectResults> {
    let devInspectTxBytes
    if (isTransaction(input.transactionBlock)) {
      input.transactionBlock.setSenderIfNotSet(input.sender)
      devInspectTxBytes = toBase64(
        await input.transactionBlock.build({
          client: this.suiClient,
          onlyTransactionKind: true,
        })
      )
    } else if (typeof input.transactionBlock === 'string') {
      devInspectTxBytes = input.transactionBlock
    } else if (input.transactionBlock instanceof Uint8Array) {
      devInspectTxBytes = toBase64(input.transactionBlock)
    } else {
      throw new Error('Unknown transaction block format.')
    }

    let executionPoint
    if ('checkpointSequenceNumber' in input.executionPoint) {
      executionPoint = {
        $kind: 'checkpoint',
        checkpointSequenceNumber: input.executionPoint.checkpointSequenceNumber,
        position: input.executionPoint.position,
      }
    } else if ('transactionDigest' in input.executionPoint) {
      executionPoint = {
        $kind: 'transaction',
        transactionDigest: input.executionPoint.transactionDigest,
        position: input.executionPoint.position,
      }
    } else {
      throw new Error('Unknown execution point.')
    }

    const body = JSON.stringify(
      {
        senderAddress: input.sender,
        txBytes: devInspectTxBytes,
        executionPoint,
        gasPrice: input.gasPrice,
      },
      null,
      2
    )

    const response = await fetch(
      `${this.baseUrl}/historical_devInspectTransactionBlock`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body,
      }
    )

    if (!response.ok) {
      throw new Error(await response.text())
    }

    return response.json()
  }
}
