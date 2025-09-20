export const BLUESKY_OSPREY_EPOCH = 1758346154000n

const machineBits = 10n
const seqBits = 12n

const machineMask = (1n << machineBits) - 1n
const seqMask = (1n << seqBits) - 1n
const seqShift = 0n
const machineShift = seqBits
const timeShift = machineBits + seqBits

export class Snowflake {
  private lastMs = 0n
  private seq = 0n
  private epoch: bigint
  private machineId: bigint

  constructor(epoch: bigint, machineId: bigint) {
    this.epoch = epoch
    this.machineId = machineId & machineMask
  }

  next(): bigint {
    while (true) {
      let now = BigInt(Date.now())
      if (now < this.lastMs) {
        continue
      }
      if (now === this.lastMs) {
        this.seq = (this.seq + 1n) & seqMask
        if (this.seq === 0n) {
          do {
            now = BigInt(Date.now())
          } while (now <= this.lastMs)
        }
      } else {
        this.seq = 0n
      }
      this.lastMs = now

      const tsPart = (now - this.epoch) << timeShift
      const machPart = (this.machineId & machineMask) << machineShift
      const seqPart = (this.seq & seqMask) << seqShift
      return tsPart | machPart | seqPart
    }
  }
}
