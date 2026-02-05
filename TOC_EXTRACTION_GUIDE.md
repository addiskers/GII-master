# Table of Contents (TOC) Extraction Methods - Complete Guide

## Overview
The `data.py` spider file contains **6 different TOC extraction methods**, each tailored to the structure of different market research websites. Here's the complete breakdown:

---

## 1. **Grand View Research** (`parse_grandview`)

**Location:** Lines 1182-1365

### How It Works:
1. **Selects the TOC container**: `'div.report_summary.full.non-indexable ul'`
2. **Recursively parses the nested UL/LI structure**
3. **Filters out regional sections** (by region, country, geography)
4. **Extracts nested sub-segments up to level 2**
5. **Only includes sub-sub-segments if there are ≥2 items**

### Key Function:
```python
def parse_and_format(ul, prefix=[]):
    """Recursively parses nested UL/LI with filtering"""
    lines = []
    for idx, li in enumerate(ul.css(':scope > li'), 1):
        # Extract text from <strong> tags or direct text
        strong_texts = li.css('strong::text, strong *::text').getall()
        if strong_texts:
            name = ' '.join(s.strip() for s in strong_texts if s.strip())
        else:
            direct_texts = li.xpath('text()').getall()
            name = ' '.join(t.strip() for t in direct_texts if t.strip())
        
        # Build numbering (1. 1.1. 1.1.1 etc)
        current_prefix = prefix + [str(idx)]
        number = ".".join(current_prefix)
        cleaned_name = self.clean_toc_entry(name)
        
        # Skip regional sections
        if self.is_regional_section(cleaned_name):
            continue
        
        # Skip long sub-segments (>6 words)
        depth = len(current_prefix)
        if depth >= 2 and len(cleaned_name.split()) > 6:
            continue
        
        lines.append(f"{number}. {cleaned_name}")
        
        # Recursively process child UL elements
        for child_ul in li.css(':scope > ul'):
            if len(current_prefix) == 2:  # Level 2
                sub_sub_lis = child_ul.css(':scope > li')
                if len(sub_sub_lis) >= 2:
                    child_lines = parse_and_format(child_ul, current_prefix)
                    lines.extend(child_lines)
            elif len(current_prefix) < 2:
                child_lines = parse_and_format(child_ul, current_prefix)
                lines.extend(child_lines)
    
    return lines
```

### Output Format:
```
1. Segment Name
1.1. Sub-segment Name
1.2. Sub-segment Name
2. Another Segment
2.1. Another Sub-segment
```

---

## 2. **Markets and Markets** (`parse_markets` + helpers)

**Location:** Lines 1365-1875

### Two Parsing Methods:

#### **2A. Accordion Format** (`parse_markets_accordion`)
- **Selector**: `div.accordion-item`
- **Main title**: `div.TOCcustHead div:nth-child(1)::text`
- **Sub-sections**: `ul.toc_list li > div.bulletsHead::text`
- **Sub-sub-sections**: `div.bullets::text`

#### **2B. Table Format** (`parse_markets_table`)
- **Selector**: `div.tblTOC div.clsTR`
- **Main**: `div.txthead::text`
- **Sub**: `div.txtsubhead::text`
- **Sub-sub**: `div:nth-child(4)::text`

### Key Features:
```python
def parse_markets_accordion(self, response):
    toc_sections = []
    for item in response.css("div.accordion-item"):
        main_title = item.css("div.TOCcustHead div:nth-child(1)::text").get()
        main_title = main_title.strip() if main_title else None
        sub_sections = []
        
        for bullet_item in item.css("ul.toc_list li"):
            main_title_elem = bullet_item.css("div.bulletsHead::text").get()
            if main_title_elem:
                main_title_text = main_title_elem.strip()
                sub_sub_sections = []
                
                for bullet in bullet_item.css("div.bullets::text"):
                    bullet_text = bullet.get().strip()
                    if bullet_text:
                        sub_sub_sections.append(bullet_text)
                
                sub_sections.append({
                    "title": main_title_text,
                    "sub_sub_sections": sub_sub_sections
                })
```

### Filtering Logic:
- **Stops keywords**: "BY REGION", "BY COUNTRY", "BY GEOGRAPHICAL"
- **Extracts**: Only sections with "BY" keyword (e.g., "BY PRODUCT TYPE")
- **Filters**: Entries with >6 words
- **Includes sub-sub-sections only if**: ≥2 items exist

### Output Format:
```
1. SEGMENT TYPE (normalized from "BY PRODUCT TYPE")
1.1. Product A
1.2. Product B
1.3. Product C
2. ANOTHER SEGMENT
2.1. Option X
2.2. Option Y
```

---

## 3. **SNS Insider** (`parse_sns`)

**Location:** Lines 1889-1980

### How It Works:
1. **Main page**: Extracts title and company profiles
2. **Segmentation page**: Makes a separate request to `/segmentation`
3. **Segmentation extraction**: (`extract_sns_segmentation_data`)
   - Finds all `<p>` tags starting with "By "
   - Collects following `<ul><li>` as subsegments
   - **Stops at**: "Regional Coverage"

### Key Code:
```python
def extract_sns_segmentation_data(self, response):
    """Extract segmentation data from div.tab-content, stopping at Regional Coverage."""
    toc = []
    segment_counter = 0
    
    for tab_content in response.css("div.tab-content"):
        p_tags = tab_content.css("p")
        for p in p_tags:
            p_text = p.xpath("string()").get()
            if not p_text:
                continue
            p_text = p_text.strip()
            
            # STOP condition
            if "Regional Coverage" in p_text:
                return toc
            
            # Only segments starting with "By"
            if not p_text.lower().startswith("by "):
                continue
            
            segment_counter += 1
            segment_title = re.sub(r'^\s*by\s+', '', p_text, flags=re.IGNORECASE).strip()
            toc.append(f"{segment_counter}. {segment_title.upper()}")
            
            # Collect subsegments from following siblings
            sub_counter = 0
            siblings = p.xpath("following-sibling::*")
            for sibling in siblings:
                sib_text = sibling.xpath("string()").get()
                if sib_text and "Regional Coverage" in sib_text:
                    return toc
                
                tag_name = sibling.root.tag
                if tag_name == "ul":
                    lis = sibling.css("li *::text, li::text").getall()
                    for li in lis:
                        li_text = li.strip()
                        if li_text:
                            sub_counter += 1
                            toc.append(f"{segment_counter}.{sub_counter}. {li_text}")
                elif tag_name == "p" and sib_text.lower().startswith("by "):
                    break
    
    return toc
```

### Output Format:
```
1. PRODUCT TYPE
1.1. Product A
1.2. Product B
2. DISTRIBUTION CHANNEL
2.1. Online
2.2. Offline
```

---

## 4. **Mordor Intelligence** (`parse_mordor`)

**Location:** Lines 1995-2200

### How It Works:
1. **Finds TOC**: `#table-of-content` selector
2. **Extracts text content**: `normalize-space(.)` to get all text
3. **Regex patterns**: Searches for `\d+\.\d+\s+By\s+[A-Za-z\s&/\-]+`
4. **Normalizes numbering**: Renumbers sequentially (converts original to 1., 1.1., 1.1.1 etc)

### Key Extraction:
```python
def extract_mordor_segmentation_data(self, toc_text):
    segmentation_lines = []
    
    # Split at "By Geography" to remove regional sections
    toc_text = re.split(r'\d+\.\d+\s+By\s+Geography', toc_text)[0]
    
    # Find all main patterns: "5.1 By Product Type"
    main_pattern = r'(\d+\.\d+\s+By\s+[A-Za-z\s&/\-]+)'
    main_matches = re.findall(main_pattern, toc_text)
    
    for main_match in main_matches:
        main_header = re.sub(r'\s+', ' ', main_match.strip())
        base_number_match = re.search(r'^(\d+\.\d+)', main_header)
        
        if not base_number_match:
            continue
        
        base_number = base_number_match.group(1)
        segmentation_lines.append(main_header)
        
        # Find sub-patterns: "5.1.1 Product Name"
        sub_pattern = rf'({re.escape(base_number)}\.\d+(?:\.\d+)*\s+[A-Za-z\s&/\-]+)'
        sub_matches = re.findall(sub_pattern, toc_text)
        
        for sub_match in sub_matches:
            segmentation_lines.append(" " + re.sub(r'\s+', ' ', sub_match.strip()))
    
    return segmentation_lines
```

### Normalization:
- Original: `5.1.4 By Product Type` → New: `1. Product Type`
- Original: `5.1.4.1 Product Name` → New: `1.1. Product Name`

---

## 5. **Fortune Business Insights** (`parse_fortune_main` → `parse_fortune_segmentation`)

**Location:** Lines 2255-2480

### How It Works:
1. **Builds segmentation URL**: Replaces `/industry-reports/` with `/industry-reports/segmentation/`
2. **Finds table**: `//div[@id="industrycoverage"]//table//tr`
3. **Extracts hierarchical structure** using symbol-based depth:
   - **Main**: Starts with "By "
   - **Level 2 (·)**: Bullet symbol `·`
   - **Level 3 (o)**: `o` symbol
   - **Level 4 (§)**: `§` symbol

### Key Parsing Logic:
```python
def parse_fortune_segmentation(self, response):
    structured_points = []
    rows = response.xpath('//div[@id="industrycoverage"]//table//tr')
    main_count = 0
    
    for row in rows:
        # Extract "By ..." text
        left_td = row.xpath('./td[1]')
        left_text = ' '.join(
            t.strip()
            for t in left_td.xpath('.//p//strong//text()').getall()
            if t.strip()
        )
        
        if not left_text.lower().startswith("by"):
            continue
        
        # Skip regions
        lower_left = left_text.lower()
        if any(k in lower_left for k in ("by region", "by geography", "by country")):
            continue
        
        main_count += 1
        segment = re.sub(r'^by\s+', '', left_text, flags=re.IGNORECASE).strip()
        structured_points.append(f"{main_count}. {segment}")
        
        # Parse sub-levels with symbols
        content_td = row.xpath('./td[2]')
        sub = sub_sub = sub_sub_sub = 0
        
        for p in content_td.xpath('.//p'):
            text = ''.join(p.xpath('.//text()').getall()).strip()
            if not text:
                continue
            
            # Level 2 — ·
            if text.startswith('·'):
                sub += 1
                sub_sub = sub_sub_sub = 0
                structured_points.append(f"{main_count}.{sub}. {text.lstrip('·').strip()}")
            
            # Level 3 — o
            elif text.startswith('o'):
                clean_text = text.lstrip('o').strip()
                if len(clean_text.split()) <= 6:
                    sub_sub += 1
                    sub_sub_sub = 0
                    structured_points.append(f"{main_count}.{sub}.{sub_sub}. {clean_text}")
            
            # Level 4 — §
            elif text.startswith('§'):
                clean_text = text.lstrip('§').strip()
                if len(clean_text.split()) <= 6:
                    sub_sub_sub += 1
                    structured_points.append(f"{main_count}.{sub}.{sub_sub}.{sub_sub_sub}. {clean_text}")
```

---

## 6. **Future Market Insights** (`parse_future`)

**Location:** Lines 2484-2640

### How It Works:
1. **Finds "Segmentation" H2 heading**: `div.report_content_div div.tab_content h2`
2. **Collects nodes until next H2**
3. **Parses hierarchical structure**:
   - **H3 tags** = Segment category
   - **UL tags** = Sub-segments
   - **Nested UL inside LI** = Sub-sub-segments (only if ≥2 items)

### Key Code:
```python
def extract_future_segmentation_as_toc(self, response):
    """Extract ONLY market segmentation data and format as numbered TOC"""
    toc_lines = []
    content_div = response.css('div.report_content_div div.tab_content')
    
    # Find Segmentation H2
    segmentation_h2 = None
    for h2 in content_div.css('h2'):
        text = h2.xpath('normalize-space(.)').get()
        if not text:
            continue
        
        text_lower = text.lower()
        if 'segmental analysis' in text_lower:
            continue
        
        if (any(word in text_lower for word in ['segment', 'segments', 'key segments'])
            and '?' not in text_lower):
            segmentation_h2 = h2
            break
    
    if not segmentation_h2:
        return toc_lines
    
    # Collect all nodes until next H2
    nodes = []
    for sib in segmentation_h2.xpath('following-sibling::*'):
        if sib.root.tag == 'h2':
            break
        nodes.append(sib)
    
    # Parse segments
    main_num = 1
    current_category = None
    
    for node in nodes:
        tag = node.root.tag
        
        # SEGMENT TITLE (H3)
        if tag == 'h3':
            raw_title = node.xpath('normalize-space(.)').get()
            if not raw_title:
                current_category = None
                continue
            
            title = raw_title.strip().rstrip(':').strip()
            if title.lower().startswith('by '):
                title = title[3:].strip()
            
            # Skip geographic sections
            if title.lower() in {'region', 'regions', 'country', 'countries'}:
                current_category = None
                continue
            
            current_category = title
        
        # SUB SEGMENTS (UL)
        elif tag == 'ul' and current_category:
            sub_num = 1
            sub_lines = []
            first_level_has_valid_items = False
            
            for li in node.xpath('./li'):
                li_text = li.xpath('normalize-space(text())').get()
                if not li_text:
                    continue
                li_text = li_text.strip()
                
                # Check for nested UL
                nested_ul = li.xpath('./ul')
                if nested_ul:
                    nested_items = []
                    for nested_li in nested_ul.xpath('./li'):
                        nested_text = nested_li.xpath('normalize-space(.)').get()
                        if nested_text:
                            nested_items.append(nested_text.strip())
                    
                    # Only add if ≥2 nested items
                    if len(nested_items) >= 2:
                        sub_lines.append(f"{main_num}.{sub_num}. {li_text}")
                        
                        sub_sub_num = 1
                        for nested_text in nested_items:
                            sub_lines.append(f"{main_num}.{sub_num}.{sub_sub_num}. {nested_text}")
                            sub_sub_num += 1
                        
                        first_level_has_valid_items = True
                else:
                    # No nested UL, treat as sub-segment
                    sub_lines.append(f"{main_num}.{sub_num}. {li_text}")
                    first_level_has_valid_items = True
                
                sub_num += 1
            
            # Add category only if has valid items
            if first_level_has_valid_items:
                toc_lines.append(f"{main_num}. {current_category}")
                toc_lines.extend(sub_lines)
                main_num += 1
            
            current_category = None
    
    return toc_lines
```

---

## 7. **Allied Market Research** (`parse_allied`)

**Location:** Lines 2640-2870

### How It Works:
1. **Extracts report_id** from page
2. **Makes API request**: `https://www.alliedmarketresearch.com/get-report-toc-rev/{report_id}`
3. **Parses accordion structure**: `#acordTabOCnt > .card`
4. **Extracts segmentation sections** from card bodies
5. **Stops at**: "BY REGION" chapter

### Key Code:
```python
def parse_allied_toc(self, response):
    flat_toc_list = []
    chapter_number = 0
    start_collecting = False
    
    for chapter_card in response.css('#acordTabOCnt > .card'):
        chapter_title = chapter_card.css('.card-header .btn-link .fw-700::text').get()
        
        if chapter_title:
            chapter_title = re.sub(r'^\d+(\.\d+)*\.\s*', '', chapter_title.strip())
            
            # Check for "Company Profiles" section
            if "company profile" in chapter_title.lower():
                # Extract company names...
                pass
            
            # Start collecting after "Market Overview"
            if not start_collecting:
                if "market overview" in chapter_title.lower() or "market landscape" in chapter_title.lower():
                    start_collecting = True
                    continue
            
            # Stop at "BY REGION"
            if " BY REGION" in chapter_title.upper():
                break
            
            # Extract segmentation
            if start_collecting:
                if " BY " in chapter_title.upper():
                    segment_type = chapter_title.upper().split(" BY ")[-1].strip()
                    chapter_number += 1
                    flat_toc_list.append(f"{chapter_number}. {segment_type}")
```

---

## Common Helper Functions

### `extract_segments_from_toc(table_of_contents)`
**Lines 73-108**

Converts flat TOC list into hierarchical segments dictionary:
```python
def extract_segments_from_toc(table_of_contents):
    """
    Input: ['1. Segment Name', '1.1. Sub-segment Name', '1.2. Sub-segment Name']
    Output: {'Segment Name': ['Sub-segment 1', 'Sub-segment 2']}
    """
    segments = {}
    current_segment = None
    
    for line in table_of_contents:
        if not isinstance(line, str):
            continue
        
        match = re.match(r'^\s*(\d+(?:\.\d+)*)[.)]?\s+(.+)$', line.strip())
        if not match:
            continue
        
        numbering = match.group(1)
        text = match.group(2).strip().title()  # Convert to title-case
        
        depth = numbering.count('.')
        
        if depth == 0:
            # Top-level segment
            current_segment = text
            if current_segment not in segments:
                segments[current_segment] = []
        elif depth == 1 and current_segment is not None:
            # Sub-segment
            segments[current_segment].append(text)
    
    # Keep only segments with sub-segments
    segments = {k: v for k, v in segments.items() if v}
    
    return segments
```

### Filtering Common Patterns
- **Regional keywords to skip**: "by region", "by country", "by geographical"
- **Max word length**: Most use 6-word limit for entries
- **Minimum sub-items**: ≥2 sub-items required to include parent
- **Stop patterns**: "Regional Coverage", "Company Profiles"

---

## Data Output Format

### JSON Structure Saved:
```json
{
  "title": "Market Name",
  "url": "source_url",
  "table_of_contents": [
    "1. Segment 1",
    "1.1. Sub-segment 1.1",
    "1.2. Sub-segment 1.2",
    "2. Segment 2",
    "2.1. Sub-segment 2.1"
  ],
  "company_profiles": ["Company A", "Company B"],
  "segments": {
    "Segment 1": ["Sub-segment 1.1", "Sub-segment 1.2"],
    "Segment 2": ["Sub-segment 2.1"]
  }
}
```

---

## Summary Table

| Source | Method | Selector | Key Feature | Stop Keyword |
|--------|--------|----------|-------------|--------------|
| Grand View | Recursive UL/LI | `div.report_summary ul` | Deep nesting, 2+ filter | Regional section |
| Markets & Markets | Accordion/Table | `div.accordion-item` | By extraction | BY REGION |
| SNS Insider | P + UL siblings | `div.tab-content p` | By pattern | Regional Coverage |
| Mordor | Regex + Text | `#table-of-content` | Regex patterns | By Geography |
| Fortune | Symbol-based | `//table//tr` | ·○§ symbols | By Region |
| Future | H3 + UL | `div.tab_content` | Nested UL filter | Next H2 |
| Allied | Card accordion | `#acordTabOCnt` | Chapter extraction | BY REGION |

