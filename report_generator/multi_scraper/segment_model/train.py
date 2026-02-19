"""
Segment Correction Model - Training Script

Learns correction patterns from the rd_submissions database where researchers
corrected AI-generated segments. Produces a local model artifact (JSON + embeddings)
that can be used for inference without any API calls.

Architecture:
1. Rule Learner - Extracts deterministic correction rules from training data
2. Similarity Engine - Uses sentence-transformers to find similar markets
3. Pattern Classifier - sklearn classifier to predict which corrections apply

Usage:
    python -m segment_model.train
"""

import sqlite3
import json
import os
import re
import pickle
import numpy as np
from collections import Counter, defaultdict
from sentence_transformers import SentenceTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_sample_weight

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'auth_users.db')
MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trained')

# ── Helpers ──────────────────────────────────────────────────────────────────

def strip_number(s):
    """Remove numbering prefix: '1.2. Foo' -> 'Foo'"""
    return re.sub(r'^\d+(?:\.\d+)*\.\s*', '', s).strip()

def parse_level(s):
    """'1.2.3. Name' -> 3"""
    m = re.match(r'^(\d+(?:\.\d+)*)\.\s', s)
    return len(m.group(1).split('.')) if m else 0

def get_numbering(s):
    """'1.2.3. Name' -> '1.2.3'"""
    m = re.match(r'^(\d+(?:\.\d+)*)\.\s', s)
    return m.group(1) if m else ''

def get_parent_number(s):
    """'1.2.3. Foo' -> '1.2'"""
    num = get_numbering(s)
    parts = num.split('.')
    return '.'.join(parts[:-1]) if len(parts) > 1 else None

def build_tree(segments):
    """Convert flat segment list to a tree structure for analysis."""
    tree = {}
    for s in segments:
        num = get_numbering(s)
        name = strip_number(s)
        level = parse_level(s)
        parent = get_parent_number(s)
        tree[num] = {
            'name': name,
            'level': level,
            'parent': parent,
            'full': s,
            'children': []
        }
    # Link children
    for num, node in tree.items():
        if node['parent'] and node['parent'] in tree:
            tree[node['parent']]['children'].append(num)
    return tree

def extract_group_features(tree, group_num):
    """Extract features for a segment group (parent with children)."""
    node = tree.get(group_num, {})
    children = node.get('children', [])
    child_names = [tree[c]['name'] for c in children if c in tree]
    
    has_others = any('other' in n.lower() for n in child_names)
    child_count = len(children)
    level = node.get('level', 0)
    name = node.get('name', '')
    
    # Check if children have their own children (grandchildren)
    has_grandchildren = any(len(tree.get(c, {}).get('children', [])) > 0 for c in children)
    
    return {
        'child_count': child_count,
        'has_others': has_others,
        'level': level,
        'has_grandchildren': has_grandchildren,
        'name': name,
        'child_names': child_names,
    }

# ── Load Data ────────────────────────────────────────────────────────────────

def load_training_data():
    """Load all submissions from DB."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT id, market_name, ai_gen_seg, segments FROM rd_submissions")
    rows = c.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        ai = json.loads(row['ai_gen_seg']) if row['ai_gen_seg'] else []
        seg = json.loads(row['segments']) if row['segments'] else []
        data.append({
            'id': row['id'],
            'market': row['market_name'],
            'ai_segments': ai,
            'correct_segments': seg,
            'is_corrected': ai != seg,
        })
    return data

# ── Rule Extraction ──────────────────────────────────────────────────────────

def extract_rules(data):
    """Extract deterministic rules from correction patterns."""
    rules = {
        # Rule 1: "Others" should be added to groups with 2+ children and no "Others"
        'add_others': {
            'description': 'Add "Others" to parent groups with 2+ children and no catch-all',
            'min_children': 2,
            'confidence': 0.0,  # Will be calculated
            'total_opportunities': 0,
            'times_applied': 0,
        },
        # Rule 2: Remove overly deep sub-segments (level 3+ that are too specific)
        'remove_deep_specific': {
            'description': 'Remove level 3+ sub-segments that are overly specific',
            'removed_names': [],  # Track which names tend to get removed
            'confidence': 0.0,
        },
        # Rule 3: Consolidate similar siblings
        'consolidate_siblings': {
            'description': 'Merge similar sub-segments into broader categories',
            'patterns': [],
        },
    }
    
    # Analyze "Others" rule
    others_opportunities = 0
    others_applied = 0
    
    for record in data:
        ai_tree = build_tree(record['ai_segments'])
        correct_tree = build_tree(record['correct_segments'])
        
        for num, node in ai_tree.items():
            children = node.get('children', [])
            if len(children) >= 2:
                others_opportunities += 1
                child_names = [ai_tree[c]['name'] for c in children if c in ai_tree]
                has_others_ai = any('other' in n.lower() for n in child_names)
                
                if not has_others_ai:
                    # Check if "Others" was added in the corrected version
                    # Find equivalent group in correct tree
                    for cnum, cnode in correct_tree.items():
                        if strip_number(cnode['full']).lower() == node['name'].lower():
                            c_children = cnode.get('children', [])
                            c_child_names = [correct_tree[cc]['name'] for cc in c_children if cc in correct_tree]
                            if any('other' in n.lower() for n in c_child_names):
                                others_applied += 1
                            break
    
    if others_opportunities > 0:
        rules['add_others']['confidence'] = others_applied / max(others_opportunities, 1)
        rules['add_others']['total_opportunities'] = others_opportunities
        rules['add_others']['times_applied'] = others_applied
    
    # Analyze removal patterns
    removed_segments = []
    for record in data:
        if not record['is_corrected']:
            continue
        ai_stripped = {strip_number(s): s for s in record['ai_segments']}
        correct_stripped = set(strip_number(s) for s in record['correct_segments'])
        
        for name, full in ai_stripped.items():
            if name not in correct_stripped:
                removed_segments.append({
                    'name': name,
                    'level': parse_level(full),
                    'market': record['market'],
                })
    
    rules['remove_deep_specific']['removed_names'] = removed_segments
    
    # Count removal frequency by level
    level_removals = Counter(r['level'] for r in removed_segments)
    rules['remove_deep_specific']['level_distribution'] = dict(level_removals)
    
    return rules

# ── Feature Engineering for Classifier ───────────────────────────────────────

def build_classifier_dataset(data, embedder):
    """Build features + labels for the segment group classifier.
    
    For each parent group in each submission, create a feature vector and label:
    - Label 1: "Others" should be added
    - Label 0: No change needed
    """
    X = []
    y = []
    meta = []
    
    for record in data:
        ai_tree = build_tree(record['ai_segments'])
        correct_tree = build_tree(record['correct_segments'])
        
        # Build a lookup for correct segment names
        correct_names = set(strip_number(s) for s in record['correct_segments'])
        ai_names = set(strip_number(s) for s in record['ai_segments'])
        
        for num, node in ai_tree.items():
            children = node.get('children', [])
            if len(children) < 1:
                continue
            
            child_names = [ai_tree[c]['name'] for c in children if c in ai_tree]
            has_others = any('other' in n.lower() for n in child_names)
            
            # Features
            features = [
                len(children),                          # child_count
                1 if has_others else 0,                 # already_has_others
                node['level'],                          # parent_level
                1 if any(len(ai_tree.get(c, {}).get('children', [])) > 0 for c in children) else 0,  # has_grandchildren
                len(node['name']),                      # name_length
                1 if 'type' in node['name'].lower() else 0,  # is_type_segment
                1 if 'application' in node['name'].lower() else 0,  # is_application
                1 if 'end' in node['name'].lower() else 0,  # is_end_user
            ]
            
            # Label: was "Others" added in the correction?
            label = 0
            if not has_others:
                # Check if corresponding group in correct version has "Others"
                for cnum, cnode in correct_tree.items():
                    if cnode['name'].lower() == node['name'].lower():
                        c_children = cnode.get('children', [])
                        c_child_names = [correct_tree[cc]['name'] for cc in c_children if cc in correct_tree]
                        if any('other' in n.lower() for n in c_child_names):
                            label = 1
                        break
            
            X.append(features)
            y.append(label)
            meta.append({
                'market': record['market'],
                'group_name': node['name'],
                'group_num': num,
            })
    
    return np.array(X), np.array(y), meta

def build_removal_classifier_dataset(data):
    """Build features + labels for whether a segment should be removed.
    
    For each segment in ai_gen_seg:
    - Label 1: segment was removed in correction
    - Label 0: segment was kept
    """
    X = []
    y = []
    
    for record in data:
        ai_tree = build_tree(record['ai_segments'])
        correct_names = set(strip_number(s) for s in record['correct_segments'])
        
        for num, node in ai_tree.items():
            name = node['name']
            level = node['level']
            children_count = len(node.get('children', []))
            parent = node.get('parent')
            
            # Sibling count
            sibling_count = 0
            if parent and parent in ai_tree:
                sibling_count = len(ai_tree[parent].get('children', []))
            
            features = [
                level,                                    # depth level
                children_count,                           # own children count
                sibling_count,                            # siblings count
                len(name),                                # name length
                len(name.split()),                        # word count
                1 if 'other' in name.lower() else 0,     # is_others
                1 if children_count == 0 else 0,          # is_leaf
                1 if level >= 3 else 0,                   # is_deep
            ]
            
            was_removed = name not in correct_names
            
            X.append(features)
            y.append(1 if was_removed else 0)
    
    return np.array(X), np.array(y)

# ── Market Embedding Index ───────────────────────────────────────────────────

def build_market_index(data, embedder):
    """Create embeddings for all markets to enable similarity search."""
    markets = []
    texts = []
    
    for record in data:
        market_name = record['market']
        # Create a rich text representation: market name + top-level segments
        top_segments = [strip_number(s) for s in record['ai_segments'] if parse_level(s) == 1]
        text = f"{market_name}: {', '.join(top_segments)}"
        markets.append({
            'market': market_name,
            'ai_segments': record['ai_segments'],
            'correct_segments': record['correct_segments'],
            'is_corrected': record['is_corrected'],
        })
        texts.append(text)
    
    embeddings = embedder.encode(texts, show_progress_bar=True, normalize_embeddings=True)
    
    return markets, embeddings

# ── Training Pipeline ────────────────────────────────────────────────────────

def train():
    print("=" * 60)
    print("Segment Correction Model - Training")
    print("=" * 60)
    
    # Step 1: Load data
    print("\n[1/6] Loading training data from DB...")
    data = load_training_data()
    corrected = [d for d in data if d['is_corrected']]
    print(f"  Total submissions: {len(data)}")
    print(f"  With corrections: {len(corrected)}")
    
    # Step 2: Extract rules
    print("\n[2/6] Extracting correction rules...")
    rules = extract_rules(data)
    print(f"  'Others' rule: {rules['add_others']['times_applied']}/{rules['add_others']['total_opportunities']} opportunities")
    print(f"  Removal patterns: {len(rules['remove_deep_specific']['removed_names'])} segments removed across all corrections")
    
    # Step 3: Load sentence-transformer
    print("\n[3/6] Loading sentence-transformer model (all-MiniLM-L6-v2)...")
    embedder = SentenceTransformer('all-MiniLM-L6-v2')
    print("  Model loaded.")
    
    # Step 4: Train "add Others" classifier
    print("\n[4/6] Training 'add Others' classifier...")
    X_others, y_others, meta_others = build_classifier_dataset(data, embedder)
    print(f"  Dataset: {len(X_others)} groups, {sum(y_others)} positive (needs Others)")
    
    scaler_others = StandardScaler()
    X_others_scaled = scaler_others.fit_transform(X_others)
    
    # Use sample weights to handle extreme class imbalance
    sample_weights_others = compute_sample_weight('balanced', y_others)
    
    clf_others = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05,
        min_samples_leaf=2,
        random_state=42,
    )
    clf_others.fit(X_others_scaled, y_others, sample_weight=sample_weights_others)
    
    # Training accuracy
    train_acc = clf_others.score(X_others_scaled, y_others)
    print(f"  Training accuracy: {train_acc:.4f}")
    
    # Step 5: Train removal classifier
    print("\n[5/6] Training segment removal classifier...")
    X_remove, y_remove = build_removal_classifier_dataset(data)
    print(f"  Dataset: {len(X_remove)} segments, {sum(y_remove)} removed")
    
    scaler_remove = StandardScaler()
    X_remove_scaled = scaler_remove.fit_transform(X_remove)
    
    sample_weights_remove = compute_sample_weight('balanced', y_remove)
    
    clf_remove = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05,
        min_samples_leaf=2,
        random_state=42,
    )
    clf_remove.fit(X_remove_scaled, y_remove, sample_weight=sample_weights_remove)
    
    remove_acc = clf_remove.score(X_remove_scaled, y_remove)
    print(f"  Training accuracy: {remove_acc:.4f}")
    
    # Step 6: Build market similarity index
    print("\n[6/6] Building market similarity index...")
    market_index, market_embeddings = build_market_index(data, embedder)
    print(f"  Indexed {len(market_index)} markets")
    
    # Save everything
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Save rules
    with open(os.path.join(MODEL_DIR, 'rules.json'), 'w', encoding='utf-8') as f:
        # Convert non-serializable parts
        rules_save = json.loads(json.dumps(rules, default=str))
        json.dump(rules_save, f, indent=2, ensure_ascii=False)
    
    # Save classifiers
    with open(os.path.join(MODEL_DIR, 'clf_others.pkl'), 'wb') as f:
        pickle.dump({'classifier': clf_others, 'scaler': scaler_others}, f)
    
    with open(os.path.join(MODEL_DIR, 'clf_remove.pkl'), 'wb') as f:
        pickle.dump({'classifier': clf_remove, 'scaler': scaler_remove}, f)
    
    # Save market index
    with open(os.path.join(MODEL_DIR, 'market_index.pkl'), 'wb') as f:
        pickle.dump({'markets': market_index, 'embeddings': market_embeddings}, f)
    
    # Save training metadata
    meta = {
        'total_submissions': len(data),
        'corrected_submissions': len(corrected),
        'others_clf_accuracy': float(train_acc),
        'remove_clf_accuracy': float(remove_acc),
        'market_count': len(market_index),
        'embedder_name': 'all-MiniLM-L6-v2',
    }
    with open(os.path.join(MODEL_DIR, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"Training complete! Model saved to: {MODEL_DIR}")
    print(f"  - rules.json")
    print(f"  - clf_others.pkl (add Others classifier)")
    print(f"  - clf_remove.pkl (removal classifier)")
    print(f"  - market_index.pkl (similarity search)")
    print(f"  - meta.json (training metadata)")
    print(f"{'='*60}")

if __name__ == '__main__':
    train()
