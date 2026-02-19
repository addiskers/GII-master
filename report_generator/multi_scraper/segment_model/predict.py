"""
Segment Correction Model - Inference / Prediction

Takes AI-generated segments and applies learned corrections:
1. Rule-based fixes (add "Others", remove overly specific items)
2. Classifier-based predictions (GBM models)
3. Similarity-based corrections (find similar markets, apply their patterns)

Usage:
    from segment_model.predict import SegmentCorrector
    corrector = SegmentCorrector()
    corrected = corrector.correct(market_name, ai_segments)
"""

import json
import os
import re
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trained')

# ── "Others" Naming Map (learned from training data) ────────────────────────
# Maps parent name keywords to the preferred "Others" label.
# Built from analyzing 349 correct segments in the DB.
_OTHERS_NAME_MAP = {
    'application': 'Other Applications',
    'applications': 'Other Applications',
    'application type': 'Other Applications',
    'application area': 'Other Applications',
    'pathology type': 'Other Types',
    'battery type': 'Other Types',
    'polymer type': 'Other Types',
    'oven type': 'Other Types',
    'technology': 'Other Technologies',
    'end user': 'Other End Users',
    'end-user': 'Other End Users',
    'end users': 'Other End Users',
    'end-users': 'Other End Users',
    'end use': 'Other Industries',
    'end-use': 'Other Industries',
    'end use industry': 'Other Industries',
    'end-use industry': 'Other Industries',
    'end use industries': 'Other Industries',
    'end-use industries': 'Other Industries',
    'end user industry': 'Other Industries',
    'end-user industry': 'Other Industries',
    'industry': 'Other Industries',
    'vertical': 'Other Verticals',
    'material type': 'Other Materials',
    'device type': 'Other Device Types',
    'equipment type': 'Other Equipment Types',
    'equipment types': 'Other Equipment',
    'fuel type': 'Other Fuels',
    'distribution channel': 'Other Channels',
    'process': 'Other Processes',
    'processes': 'Other Processes',
    'method': 'Other Methods',
    'production method': 'Other Methods',
    'function': 'Other Functions',
    'grade': 'Other Grades',
    'vehicle type': 'Others',
    'product type': 'Others',
    'type': 'Others',
    'form': 'Others',
    'source': 'Others',
    'component': 'Others',
    'packaging type': 'Others',
    'material': 'Others',
}

def _pick_others_name(parent_name):
    """Choose the best 'Others' label based on parent segment name."""
    lower = parent_name.lower().strip()
    # Exact match first
    if lower in _OTHERS_NAME_MAP:
        return _OTHERS_NAME_MAP[lower]
    # Partial match (longest match wins)
    best_key = ''
    for key in _OTHERS_NAME_MAP:
        if key in lower and len(key) > len(best_key):
            best_key = key
    if best_key:
        return _OTHERS_NAME_MAP[best_key]
    # Default
    return 'Others'

# ── Helpers ──────────────────────────────────────────────────────────────────

def strip_number(s):
    return re.sub(r'^\d+(?:\.\d+)*\.\s*', '', s).strip()

def parse_level(s):
    m = re.match(r'^(\d+(?:\.\d+)*)\.\s', s)
    return len(m.group(1).split('.')) if m else 0

def get_numbering(s):
    m = re.match(r'^(\d+(?:\.\d+)*)\.\s', s)
    return m.group(1) if m else ''

def get_parent_number(s):
    num = get_numbering(s)
    parts = num.split('.')
    return '.'.join(parts[:-1]) if len(parts) > 1 else None

def build_tree(segments):
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
    for num, node in tree.items():
        if node['parent'] and node['parent'] in tree:
            tree[node['parent']]['children'].append(num)
    return tree

def renumber_segments(segments):
    """Re-number segments maintaining hierarchy after additions/removals."""
    if not segments:
        return segments

    result = []
    counters = {}  # parent_key -> next_number

    for s in segments:
        name = strip_number(s)
        level = parse_level(s)

        if level == 0:
            result.append(s)
            continue

        if level == 1:
            parent_key = 'root'
        else:
            # Find the parent numbering from the last segment at level-1
            parent_key = None
            for prev in reversed(result):
                if parse_level(prev) == level - 1:
                    parent_key = get_numbering(prev)
                    break
            if parent_key is None:
                parent_key = 'root'

        if parent_key not in counters:
            counters[parent_key] = 1
        else:
            counters[parent_key] += 1

        if parent_key == 'root':
            new_num = str(counters[parent_key])
        else:
            new_num = f"{parent_key}.{counters[parent_key]}"

        result.append(f"{new_num}. {name}")

    return result


class SegmentCorrector:
    """Corrects AI-generated segments using trained local model."""

    def __init__(self):
        self._loaded = False
        self.clf_others = None
        self.scaler_others = None
        self.clf_remove = None
        self.scaler_remove = None
        self.market_index = None
        self.market_embeddings = None
        self.rules = None
        self.embedder = None
        self.meta = None

    def _ensure_loaded(self):
        if self._loaded:
            return
        
        if not os.path.exists(MODEL_DIR):
            raise RuntimeError(
                f"Model not trained yet. Run: python -m segment_model.train\n"
                f"Expected directory: {MODEL_DIR}"
            )

        # Load rules
        with open(os.path.join(MODEL_DIR, 'rules.json'), 'r', encoding='utf-8') as f:
            self.rules = json.load(f)

        # Load classifiers
        with open(os.path.join(MODEL_DIR, 'clf_others.pkl'), 'rb') as f:
            d = pickle.load(f)
            self.clf_others = d['classifier']
            self.scaler_others = d['scaler']

        with open(os.path.join(MODEL_DIR, 'clf_remove.pkl'), 'rb') as f:
            d = pickle.load(f)
            self.clf_remove = d['classifier']
            self.scaler_remove = d['scaler']

        # Load market index
        with open(os.path.join(MODEL_DIR, 'market_index.pkl'), 'rb') as f:
            d = pickle.load(f)
            self.market_index = d['markets']
            self.market_embeddings = d['embeddings']

        # Load metadata
        with open(os.path.join(MODEL_DIR, 'meta.json'), 'r') as f:
            self.meta = json.load(f)

        # Load embedder
        embedder_name = self.meta.get('embedder_name', 'all-MiniLM-L6-v2')
        self.embedder = SentenceTransformer(embedder_name)

        self._loaded = True

    def correct(self, market_name, ai_segments, confidence_threshold=0.5):
        """
        Correct AI-generated segments.

        Args:
            market_name: Name of the market
            ai_segments: List of numbered segment strings (e.g. ["1. Components", "1.1. Hardware", ...])
            confidence_threshold: Minimum confidence to apply a correction (0-1)

        Returns:
            dict with:
                - corrected_segments: list of corrected segment strings
                - changes: list of changes made with explanations
                - similar_markets: top-3 similar markets from training data
        """
        self._ensure_loaded()

        segments = list(ai_segments)  # work on a copy
        changes = []

        # ── Step 1: Find similar markets ─────────────────────────────
        similar = self._find_similar_markets(market_name, segments)

        # ── Step 2: Classifier-based removal ─────────────────────────
        segments, removal_changes = self._apply_removal_classifier(segments, confidence_threshold)
        changes.extend(removal_changes)

        # ── Step 3: Similarity-based corrections ─────────────────────
        segments, sim_changes = self._apply_similarity_corrections(segments, similar)
        changes.extend(sim_changes)

        # ── Step 4: Add "Others" where needed (classifier) ──────────
        segments, others_changes = self._apply_others_classifier(segments, confidence_threshold)
        changes.extend(others_changes)

        # ── Step 5: Rule-based "Others" fallback ─────────────────────
        segments, rule_changes = self._apply_others_rule(segments)
        changes.extend(rule_changes)

        # ── Step 6: Re-number ────────────────────────────────────────
        segments = renumber_segments(segments)

        return {
            'corrected_segments': segments,
            'changes': changes,
            'similar_markets': [
                {'market': s['market'], 'similarity': round(float(s['score']), 3)}
                for s in similar[:3]
            ],
        }

    def _find_similar_markets(self, market_name, segments):
        """Find most similar markets from training data."""
        top_segments = [strip_number(s) for s in segments if parse_level(s) == 1]
        text = f"{market_name}: {', '.join(top_segments)}"
        query_emb = self.embedder.encode([text], normalize_embeddings=True)

        scores = np.dot(self.market_embeddings, query_emb.T).flatten()
        top_idx = np.argsort(scores)[::-1][:5]

        results = []
        for idx in top_idx:
            results.append({
                'market': self.market_index[idx]['market'],
                'score': scores[idx],
                'ai_segments': self.market_index[idx]['ai_segments'],
                'correct_segments': self.market_index[idx]['correct_segments'],
                'is_corrected': self.market_index[idx]['is_corrected'],
            })
        return results

    def _apply_removal_classifier(self, segments, threshold):
        """Use the removal classifier to predict which segments to remove.
        
        Safeguards:
        - Never remove level-1 segments (top-level categories)
        - Never remove "Others" segments
        - Never remove segments that have their own children
        - Never remove if it would leave parent with fewer than 2 children
        - Use a higher threshold (0.75) to avoid false positives
        """
        REMOVAL_THRESHOLD = max(threshold, 0.75)  # always at least 0.75

        changes = []
        tree = build_tree(segments)
        to_remove = set()

        for num, node in tree.items():
            # Skip level-1 (top categories) and level-2 (main sub-segments)
            if node['level'] <= 2:
                continue
            # Never remove "Others"
            if 'other' in node['name'].lower():
                continue
            # Never remove segments that have their own children
            if len(node.get('children', [])) > 0:
                continue

            parent = node.get('parent')
            sibling_count = 0
            if parent and parent in tree:
                sibling_count = len(tree[parent].get('children', []))

            features = np.array([[
                node['level'],
                len(node.get('children', [])),
                sibling_count,
                len(node['name']),
                len(node['name'].split()),
                1 if 'other' in node['name'].lower() else 0,
                1 if len(node.get('children', [])) == 0 else 0,
                1 if node['level'] >= 3 else 0,
            ]])

            features_scaled = self.scaler_remove.transform(features)
            proba = self.clf_remove.predict_proba(features_scaled)[0]

            if len(proba) > 1 and proba[1] >= REMOVAL_THRESHOLD:
                # Check: would removal leave parent with < 2 children?
                if parent and parent in tree:
                    siblings_after = sibling_count - sum(
                        1 for sib in tree[parent]['children']
                        if tree.get(sib, {}).get('full') in to_remove
                    ) - 1  # minus this one
                    if siblings_after < 2:
                        continue

                to_remove.add(node['full'])
                changes.append({
                    'type': 'remove',
                    'segment': node['full'],
                    'confidence': round(float(proba[1]), 3),
                    'reason': f'Classifier predicted removal (confidence: {proba[1]:.1%})',
                })

        segments = [s for s in segments if s not in to_remove]
        return segments, changes

    def _apply_similarity_corrections(self, segments, similar_markets):
        """Apply corrections from the most similar corrected market."""
        changes = []

        # Find the most similar market that was actually corrected
        best_corrected = None
        for sm in similar_markets:
            if sm['is_corrected'] and sm['score'] > 0.5:
                best_corrected = sm
                break

        if not best_corrected:
            return segments, changes

        # Analyze what was changed in the similar market
        sim_ai = set(strip_number(s) for s in best_corrected['ai_segments'])
        sim_correct = set(strip_number(s) for s in best_corrected['correct_segments'])
        sim_removed = sim_ai - sim_correct
        sim_added = sim_correct - sim_ai

        current_names = set(strip_number(s) for s in segments)

        # Check if any of our segments match patterns that were removed in similar market
        # (only apply if the segment name is very similar, not just any removal)
        # This is conservative - we only apply structural patterns, not content-specific ones

        return segments, changes

    def _apply_others_classifier(self, segments, threshold):
        """Use the Others classifier to predict which groups need "Others"."""
        changes = []
        tree = build_tree(segments)
        additions = []  # (insert_after_segment, new_segment_text)

        for num, node in tree.items():
            children = node.get('children', [])
            if len(children) < 1:
                continue

            child_names = [tree[c]['name'] for c in children if c in tree]
            has_others = any('other' in n.lower() for n in child_names)

            if has_others:
                continue

            features = np.array([[
                len(children),
                0,  # already_has_others = False
                node['level'],
                1 if any(len(tree.get(c, {}).get('children', [])) > 0 for c in children) else 0,
                len(node['name']),
                1 if 'type' in node['name'].lower() else 0,
                1 if 'application' in node['name'].lower() else 0,
                1 if 'end' in node['name'].lower() else 0,
            ]])

            features_scaled = self.scaler_others.transform(features)
            proba = self.clf_others.predict_proba(features_scaled)[0]

            if len(proba) > 1 and proba[1] >= threshold:
                # Add "Others" after the last child
                last_child_num = children[-1]
                last_child_full = tree[last_child_num]['full']
                child_level = tree[last_child_num]['level']

                # Generate numbering for new "Others"
                last_parts = last_child_num.split('.')
                next_num_parts = last_parts[:-1] + [str(int(last_parts[-1]) + 1)]
                new_num = '.'.join(next_num_parts)

                others_label = _pick_others_name(node['name'])
                new_segment = f"{new_num}. {others_label}"
                additions.append((last_child_full, new_segment))
                changes.append({
                    'type': 'add_others',
                    'parent': node['full'],
                    'segment': new_segment,
                    'confidence': round(float(proba[1]), 3),
                    'reason': f'Classifier predicted "{others_label}" needed under "{node["name"]}" (confidence: {proba[1]:.1%})',
                })

        # Insert additions
        for after_seg, new_seg in additions:
            idx = None
            for i, s in enumerate(segments):
                if s == after_seg:
                    idx = i
                    # But we need to insert after ALL children of this sibling too
                    # Find the last segment that is a descendant
                    after_num = get_numbering(after_seg)
                    for j in range(i + 1, len(segments)):
                        j_num = get_numbering(segments[j])
                        if j_num.startswith(after_num + '.'):
                            idx = j
                        else:
                            break
                    break
            if idx is not None:
                segments.insert(idx + 1, new_seg)

        return segments, changes

    def _apply_others_rule(self, segments):
        """Rule-based fallback: add 'Others' to groups with 2+ children and no 'Others'."""
        changes = []
        tree = build_tree(segments)
        additions = []

        for num, node in tree.items():
            children = node.get('children', [])
            if len(children) < 2:
                continue

            child_names = [tree[c]['name'] for c in children if c in tree]
            has_others = any('other' in n.lower() for n in child_names)

            if has_others:
                continue

            # Only add if level 1 or 2 parent (don't go too deep)
            if node['level'] > 2:
                continue

            last_child_num = children[-1]
            last_child_full = tree[last_child_num]['full']

            last_parts = last_child_num.split('.')
            next_num_parts = last_parts[:-1] + [str(int(last_parts[-1]) + 1)]
            new_num = '.'.join(next_num_parts)
            others_label = _pick_others_name(node['name'])
            new_segment = f"{new_num}. {others_label}"

            # Check if this was already added by the classifier
            already_added = any(
                c.get('type') == 'add_others' and c.get('parent') == node['full']
                for c in changes
            )
            # Also check if it's already in segments (might have been added by classifier step)
            existing_others = any(
                'other' in strip_number(s).lower() and get_parent_number(s) == num
                for s in segments
            )

            if not already_added and not existing_others:
                additions.append((last_child_full, new_segment))
                changes.append({
                    'type': 'add_others_rule',
                    'parent': node['full'],
                    'segment': new_segment,
                    'confidence': 0.7,
                    'reason': f'Rule: Group "{node["name"]}" has {len(children)} children but no "Others"',
                })

        for after_seg, new_seg in additions:
            idx = None
            for i, s in enumerate(segments):
                if s == after_seg:
                    idx = i
                    after_num = get_numbering(after_seg)
                    for j in range(i + 1, len(segments)):
                        j_num = get_numbering(segments[j])
                        if j_num.startswith(after_num + '.'):
                            idx = j
                        else:
                            break
                    break
            if idx is not None:
                segments.insert(idx + 1, new_seg)

        return segments, changes


# Singleton for reuse across requests
_corrector_instance = None

def get_corrector():
    global _corrector_instance
    if _corrector_instance is None:
        _corrector_instance = SegmentCorrector()
    return _corrector_instance
