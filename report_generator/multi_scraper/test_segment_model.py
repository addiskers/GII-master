"""Test the trained segment correction model."""
import json
import sqlite3
import os
import re

from segment_model.predict import get_corrector

def strip_number(s):
    return re.sub(r'^\d+(?:\.\d+)*\.\s*', '', s).strip()

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'auth_users.db')

# Test 1: Use a known corrected market from DB
print("=" * 60)
print("TEST 1: Known corrected market (AR and VR in Interventional Neuroradiology)")
print("=" * 60)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
c = conn.cursor()
c.execute("SELECT market_name, ai_gen_seg, segments FROM rd_submissions WHERE id = 7")
row = c.fetchone()

ai_segs = json.loads(row['ai_gen_seg'])
correct_segs = json.loads(row['segments'])

print(f"\nOriginal AI segments ({len(ai_segs)}):")
for s in ai_segs:
    print(f"  {s}")

print(f"\nExpected correct segments ({len(correct_segs)}):")
for s in correct_segs:
    print(f"  {s}")

corrector = get_corrector()
result = corrector.correct(row['market_name'], ai_segs)

print(f"\nModel corrected segments ({len(result['corrected_segments'])}):")
for s in result['corrected_segments']:
    print(f"  {s}")

print(f"\nChanges made ({len(result['changes'])}):")
for ch in result['changes']:
    print(f"  [{ch['type']}] {ch['reason']}")

print(f"\nSimilar markets:")
for sm in result['similar_markets']:
    print(f"  {sm['market']} (similarity: {sm['similarity']})")

# Test 2: Completely new market
print("\n" + "=" * 60)
print("TEST 2: Brand new market (Electric Vehicle Battery Market)")
print("=" * 60)

new_ai_segments = [
    "1. Battery Type",
    "1.1. Lithium-Ion",
    "1.2. Solid-State",
    "1.3. Nickel-Metal Hydride",
    "2. Vehicle Type",
    "2.1. Battery Electric Vehicle (BEV)",
    "2.2. Plug-in Hybrid Electric Vehicle (PHEV)",
    "2.3. Hybrid Electric Vehicle (HEV)",
    "3. Application",
    "3.1. Passenger Cars",
    "3.2. Commercial Vehicles",
    "3.3. Two-Wheelers",
    "4. Component",
    "4.1. Cathode",
    "4.2. Anode",
    "4.3. Electrolyte",
    "4.4. Separator",
]

result2 = corrector.correct("Electric Vehicle Battery Market", new_ai_segments)

print(f"\nOriginal ({len(new_ai_segments)}) -> Corrected ({len(result2['corrected_segments'])})")
print(f"\nCorrected segments:")
for s in result2['corrected_segments']:
    print(f"  {s}")

print(f"\nChanges made ({len(result2['changes'])}):")
for ch in result2['changes']:
    print(f"  [{ch['type']}] {ch['reason']}")

print(f"\nSimilar markets:")
for sm in result2['similar_markets']:
    print(f"  {sm['market']} (similarity: {sm['similarity']})")

# Test 3: Accuracy on all 23 corrected records
print("\n" + "=" * 60)
print("TEST 3: Accuracy on all corrected records")
print("=" * 60)

c.execute("SELECT id, market_name, ai_gen_seg, segments FROM rd_submissions")
all_rows = c.fetchall()

total_corrected = 0
others_added_correctly = 0
others_added_total = 0
removal_correct = 0
removal_total = 0

for row in all_rows:
    ai = json.loads(row['ai_gen_seg']) if row['ai_gen_seg'] else []
    correct = json.loads(row['segments']) if row['segments'] else []
    
    if ai == correct:
        continue
    
    total_corrected += 1
    result = corrector.correct(row['market_name'], ai)
    
    # Check: did we add "Others" where it was needed?
    correct_others = set()
    ai_names = set(s for s in ai)
    for s in correct:
        if s not in ai_names and 'other' in s.lower():
            correct_others.add(s)
    
    model_others = set()
    for ch in result['changes']:
        if 'others' in ch['type']:
            model_others.add(ch.get('segment', ''))
    
    if correct_others:
        others_added_total += len(correct_others)
        # Check overlap (by name, ignoring numbering)
        correct_others_names = set(strip_number(s) for s in correct_others)
        model_others_names = set(strip_number(s) for s in model_others)
        others_added_correctly += len(correct_others_names & model_others_names)

print(f"\nTotal corrected records tested: {total_corrected}")
print(f"'Others' additions: {others_added_correctly}/{others_added_total} correct")

conn.close()
print("\nAll tests complete!")
