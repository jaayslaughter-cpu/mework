-- V38: Backfill incorrect entry_type labels from pre-PR#453 dispatches.
--
-- Historical labels that shipped wrong:
--   "POWER"      → PowerPlay  (April 16 dispatches, UD standard 2-3 leg)
--   "STANDARD"   → PowerPlay  (April 16 dispatches, UD standard — not flex)
--   "Power Play" → PowerPlay  (April 20-21 dispatches, space in label)
--
-- All confirmed Underdog rows (platform = 'underdog').
-- We have no historical FlexPlay rows (no 4-5 leg slips ever dispatched),
-- so mapping all three old labels → PowerPlay is safe.

UPDATE bet_ledger
   SET entry_type = 'PowerPlay'
 WHERE platform = 'underdog'
   AND entry_type IN ('POWER', 'STANDARD', 'Power Play');

-- PrizePicks rows that may have shipped with wrong labels
UPDATE bet_ledger
   SET entry_type = 'Power'
 WHERE platform = 'prizepicks'
   AND entry_type IN ('POWER', 'STANDARD', 'standard', 'power');
