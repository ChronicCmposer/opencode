// Deliberately crashes before the executor's ready frame so the hub-side
// executor reports a crash (never a request timeout).

throw new Error("intentional db executor fixture crash")
