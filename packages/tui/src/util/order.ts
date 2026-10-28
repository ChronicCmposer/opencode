/** Deterministic ordering independent of host locale and ICU data. */
export function compareCodepoint(left: string, right: string): number {
  if (left === right) return 0
  const leftPoints = Array.from(left, (character) => character.codePointAt(0)!)
  const rightPoints = Array.from(right, (character) => character.codePointAt(0)!)
  const shared = Math.min(leftPoints.length, rightPoints.length)
  for (let index = 0; index < shared; index++) {
    const difference = leftPoints[index]! - rightPoints[index]!
    if (difference !== 0) return difference < 0 ? -1 : 1
  }
  return leftPoints.length - rightPoints.length
}

const DIGIT = /[0-9]/

/** Code-point order with digit runs compared numerically (`part2` before `part10`). */
export function compareCodepointNumeric(left: string, right: string): number {
  if (left === right) return 0
  let leftIndex = 0
  let rightIndex = 0
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftCharacter = left[leftIndex]!
    const rightCharacter = right[rightIndex]!
    if (DIGIT.test(leftCharacter) && DIGIT.test(rightCharacter)) {
      const leftEnd = digitRunEnd(left, leftIndex)
      const rightEnd = digitRunEnd(right, rightIndex)
      const leftDigits = left.slice(leftIndex, leftEnd).replace(/^0+(?=\d)/, "")
      const rightDigits = right.slice(rightIndex, rightEnd).replace(/^0+(?=\d)/, "")
      if (leftDigits.length !== rightDigits.length) return leftDigits.length < rightDigits.length ? -1 : 1
      if (leftDigits !== rightDigits) return leftDigits < rightDigits ? -1 : 1
      leftIndex = leftEnd
      rightIndex = rightEnd
      continue
    }
    const difference = leftCharacter.codePointAt(0)! - rightCharacter.codePointAt(0)!
    if (difference !== 0) return difference < 0 ? -1 : 1
    leftIndex += leftCharacter.length
    rightIndex += rightCharacter.length
  }
  return left.length - leftIndex - (right.length - rightIndex)
}

function digitRunEnd(value: string, start: number): number {
  let end = start
  while (end < value.length && DIGIT.test(value[end]!)) end++
  return end
}
