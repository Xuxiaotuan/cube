import { AtomicPreAggregationLedger, LedgerRecord, LedgerCommand } from '../src/AtomicPreAggregationLedger';

const record: LedgerRecord = {
  key: 'build', generation: '9007199254740993', owner: 'router',
  leaseUntilMillis: '1800000000000', state: 'claimed', manifest: null, references: '0'
};
const claim: LedgerCommand = { op: 'claim', key: 'build', owner: 'router', leaseMillis: '30000', expectedGeneration: null };

describe('Atomic pre-aggregation ledger protocol', () => {
  test('preserves request identity and large integer strings', async () => {
    const transport = jest.fn().mockResolvedValue({ record, rejection: null });
    const ledger = new AtomicPreAggregationLedger(transport);
    expect((await ledger.mutate('request-1', claim)).record?.generation).toBe('9007199254740993');
    expect(transport).toHaveBeenCalledWith('POST', '/router/pre-aggregation-ledger', { requestId: 'request-1', command: claim });
  });

  test('does not retry ambiguous mutations', async () => {
    const transport = jest.fn().mockRejectedValue(new Error('connection lost'));
    await expect(new AtomicPreAggregationLedger(transport).mutate('same-id', claim)).rejects.toThrow('connection lost');
    expect(transport).toHaveBeenCalledTimes(1);
  });

  test('returns explicit business rejection rather than successful recovery', async () => {
    const result = { record: null, rejection: 'stale-generation' };
    const ledger = new AtomicPreAggregationLedger(jest.fn().mockResolvedValue(result));
    await expect(ledger.mutate('rejected-id', claim)).resolves.toEqual(result);
  });

  test.each([null, {}, { record: null, rejection: null }, { record: { ...record, generation: 1 }, rejection: null }])(
    'rejects malformed mutation receipts: %j', async (response) => {
      const ledger = new AtomicPreAggregationLedger(jest.fn().mockResolvedValue(response));
      await expect(ledger.mutate('receipt-id', claim)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    }
  );

  test('encodes lookup keys and does not reinterpret missing as safe SQL replay', async () => {
    const transport = jest.fn().mockResolvedValue(null);
    await expect(new AtomicPreAggregationLedger(transport).read('a/b?x', '9007199254740993')).resolves.toBeNull();
    expect(transport).toHaveBeenCalledWith('GET', '/router/pre-aggregation-ledger?key=a%2Fb%3Fx&generation=9007199254740993');
  });

  test('requires the caller to provide a stable mutation identity', async () => {
    const transport = jest.fn();
    await expect(new AtomicPreAggregationLedger(transport).mutate('', claim)).rejects.toThrow('requestId');
    expect(transport).not.toHaveBeenCalled();
  });
});
