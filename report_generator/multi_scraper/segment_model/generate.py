"""
Segment Generation Pipeline - Market Name → Corrected Segments

Combines:
1. OpenAI AI segment generation (from market name)
2. Local correction model (adds Others, fixes naming, removes overly specific items)

Usage (CLI):
    python -m segment_model.generate "Electric Vehicle Battery Market"
    python -m segment_model.generate "3D Imaging Market" --confidence 0.6

Usage (Python):
    from segment_model.generate import generate_segments
    result = generate_segments("Electric Vehicle Battery Market")
    print(result['corrected_segments'])
"""

import os
import re
import sys
import json

# Ensure parent dir is on path for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(BASE_DIR, '.env'))


def _flatten_nested_to_numbered(segments_data):
    """Convert nested AI segment JSON to flat numbered strings.
    
    Input:  {"segments": [{"name": "Type", "subsegments": [{"name": "Foo", ...}]}]}
    Output: ["1. Type", "1.1. Foo", ...]
    """
    result = []

    def traverse(segments, prefix=''):
        for idx, segment in enumerate(segments):
            if not isinstance(segment, dict):
                continue
            num = f"{prefix}{idx + 1}" if prefix else f"{idx + 1}"
            name = segment.get('name', '').strip()
            if name:
                result.append(f"{num}. {name}")
            subs = segment.get('subsegments', [])
            if subs:
                traverse(subs, prefix=f"{num}.")

    if isinstance(segments_data, dict) and 'segments' in segments_data:
        traverse(segments_data['segments'])
    elif isinstance(segments_data, list):
        traverse(segments_data)

    return result


def _generate_ai_segments_raw(market_name):
    """Call OpenAI to generate raw nested segments from market name.
    
    Returns dict with 'segments' key (nested JSON) or 'error' key.
    """
    try:
        from openai import OpenAI
    except ImportError:
        return {'error': 'openai package not installed. Run: pip install openai'}

    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        return {'error': 'OPENAI_API_KEY not set in environment or .env file'}

    client = OpenAI(api_key=api_key)
    model_name = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')

    prompt = f"""
        You are a market research expert who creates concise and logical market segmentation hierarchies.

        Based on the following title: "{market_name}"

        Generate a clear and relevant **market segmentation structure** suitable for a professional market research report.

        ### Guidelines:
        - Create **4 to 5 main segments (Level 1)** depending on the market scope.
        - Each main segment may include **2–3 subsegments (Level 2)**.
        - Add deeper levels (**Level 2 or Level 3**) **only if necessary and meaningful** — do not force full depth if the topic is narrow and in each level.
        - Each level should become **more specific and detailed** than the previous one.
        - Avoid unnecessary repetition or overly granular splits.
        - Keep the structure **realistic, business-oriented, and readable**.
        - keep the names in maximum 3-4 words dont exceed it.
        - Dont include geographic or regional segments.
        - and at every level dont give only one point give atlest 2 points or more.

        ### Format (Strict JSON):
        {{
        "segments": [
            {{
            "name": "Level 1 Segment Name",
            "subsegments": [
                {{
                "name": "Level 2 Sub-segment Name",
                "subsegments": [
                    {{
                    "name": "Level 3 Sub-sub-segment Name",
                    "subsegments": []
                    }}
                ]
                }}
            ]
            }}
        ]
        }}

        ### Output Rules:
        - Respond **only with valid JSON** (no explanations, text, or notes).
        - Structure depth and number of segments should **match the complexity** of "{market_name}".
        - Be relevant, realistic, and concise.
        """

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a market research expert who creates detailed market segmentation structures. Always respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        content = response.choices[0].message.content.strip()
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        else:
            return json.loads(content)
    except json.JSONDecodeError as e:
        return {'error': f'Failed to parse AI response: {str(e)}'}
    except Exception as e:
        return {'error': f'AI generation failed: {str(e)}'}


def generate_segments(market_name, confidence=0.5):
    """Full pipeline: market name → AI segments → correction model → result.
    
    Args:
        market_name: Name of the market (e.g. "Electric Vehicle Battery Market")
        confidence: Confidence threshold for corrections (0-1, default 0.5)
    
    Returns:
        dict with:
            - corrected_segments: list of corrected numbered segment strings
            - ai_raw_segments: list of original AI-generated numbered strings
            - changes: list of corrections applied
            - similar_markets: top-3 similar markets from training data
            - error: only if something went wrong
    """
    # Step 1: Generate AI segments from market name
    ai_result = _generate_ai_segments_raw(market_name)
    if 'error' in ai_result:
        return ai_result

    # Step 2: Flatten nested JSON to numbered strings
    ai_flat = _flatten_nested_to_numbered(ai_result)
    if not ai_flat:
        return {'error': 'AI generated empty segments'}

    # Step 3: Apply correction model
    from segment_model.predict import get_corrector
    corrector = get_corrector()
    correction = corrector.correct(market_name, ai_flat, confidence_threshold=confidence)

    return {
        'corrected_segments': correction['corrected_segments'],
        'ai_raw_segments': ai_flat,
        'changes': correction['changes'],
        'similar_markets': correction['similar_markets'],
    }


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Generate corrected market segments from a market name',
        usage='python -m segment_model.generate "Market Name" [--confidence 0.5]'
    )
    parser.add_argument('market_name', help='Name of the market')
    parser.add_argument('--confidence', type=float, default=0.5,
                        help='Confidence threshold for corrections (default: 0.5)')
    parser.add_argument('--json', action='store_true',
                        help='Output as JSON instead of formatted text')
    args = parser.parse_args()

    print(f"Generating segments for: {args.market_name}")
    print(f"Confidence threshold: {args.confidence}")
    print("=" * 60)

    result = generate_segments(args.market_name, confidence=args.confidence)

    if 'error' in result:
        print(f"\nERROR: {result['error']}")
        sys.exit(1)

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        sys.exit(0)

    print(f"\nAI Generated Segments ({len(result['ai_raw_segments'])}):")
    for s in result['ai_raw_segments']:
        print(f"  {s}")

    print(f"\nCorrected Segments ({len(result['corrected_segments'])}):")
    for s in result['corrected_segments']:
        print(f"  {s}")

    if result['changes']:
        print(f"\nChanges Applied ({len(result['changes'])}):")
        for ch in result['changes']:
            print(f"  [{ch['type']}] {ch['reason']}")

    if result['similar_markets']:
        print(f"\nSimilar Markets:")
        for sm in result['similar_markets']:
            print(f"  {sm['market']} (similarity: {sm['similarity']})")


if __name__ == '__main__':
    main()
