# Complete TOC Extraction Source Code - All Methods

---

## 1. GRAND VIEW RESEARCH

### Full Source Code (Lines 1182-1365)

```python
def parse_grandview(self, response):
    summary = response.css('div.report_summary.full.non-indexable ul')
 
    # Extract company profiles from the main report page (not segmentation page)
    company_profiles = []
    original_url = response.meta.get('original_url', response.url.replace('/segmentation', ''))
 
    # Only fetch company profiles if we have a different URL than the current one
    if original_url != response.url:
        yield scrapy.Request(original_url,
                           callback=self.parse_grandview_company_profiles,
                           meta={'response_data': response, 'original_url': original_url})
        return
 
    # If we're already on the main page, process normally
    self.process_grandview_data(response, summary, company_profiles)

def parse_grandview_company_profiles(self, response):
    """Parse company profiles from the main report page"""
    response_data = response.meta['response_data']
    original_url = response.meta['original_url']
 
    # Extract company profiles
    company_profiles = self.extract_grandview_company_profiles(response)
 
    # Process the final data with company profiles
    self.process_grandview_data(response_data, response_data.css('div.report_summary.full.non-indexable ul'), company_profiles)

def process_grandview_data(self, response, summary, company_profiles):
    """Process and save the final data"""
    if summary:
        def parse_and_format(ul, prefix=[]):
            lines = []
            for idx, li in enumerate(ul.css(':scope > li'), 1):
                # ---- extract name ----
                strong_texts = li.css('strong::text, strong *::text').getall()
                if strong_texts:
                    name = ' '.join(s.strip() for s in strong_texts if s.strip())
                else:
                    direct_texts = li.xpath('text()').getall()
                    name = ' '.join(t.strip() for t in direct_texts if t.strip())
                if not name:
                    continue

                current_prefix = prefix + [str(idx)]
                number = ".".join(current_prefix)
                cleaned_name = self.clean_toc_entry(name)
                
                # FILTERING: Skip regional sections
                if self.is_regional_section(cleaned_name):
                    continue
                
                depth = len(current_prefix)
                
                # FILTERING: Skip sub/sub-sub with >6 words
                if depth >= 2 and len(cleaned_name.split()) > 6:
                    continue
                
                # Add segment & sub-segment
                lines.append(f"{number}. {cleaned_name}")
                
                # ---- handle children ----
                for child_ul in li.css(':scope > ul'):
                    # CASE: sub-sub segments (depth == 2)
                    if len(current_prefix) == 2:
                        sub_sub_lis = child_ul.css(':scope > li')
                        # FILTERING: append ONLY if count > 2
                        if len(sub_sub_lis) >= 2:
                            child_lines = parse_and_format(child_ul, current_prefix)
                            lines.extend([
                                line for line in child_lines
                                if not self.is_regional_line(line)
                            ])
                    # CASE: segment → sub-segment (depth < 2)
                    elif len(current_prefix) < 2:
                        child_lines = parse_and_format(child_ul, current_prefix)
                        lines.extend([
                            line for line in child_lines
                            if not self.is_regional_line(line)
                        ])

            return lines
        
        numbered_lines = parse_and_format(summary[0])
        # Filter out any remaining regional lines that might have slipped through
        filtered_lines = [line for line in numbered_lines if not self.is_regional_line(line)]
        
        # Clean the title
        raw_title = response.css('title::text').get()
        cleaned_title = self.clean_title(raw_title)
        self.market_name = cleaned_title.replace(" Market", "").strip()
        
        # Clean all TOC entries
        cleaned_lines = [self.clean_toc_line(line) for line in filtered_lines]
        
        # Extract segments from cleaned_lines
        segments = extract_segments_from_toc(cleaned_lines)
        
        # Prepare the data with both table_of_contents and company_profiles
        data = {
            'title': cleaned_title,
            'url': response.meta.get('original_url', response.url),
            'table_of_contents': cleaned_lines
        }
        
        # Add segments if available
        if segments:
            data['segments'] = segments
        
        # Add company profiles if available
        if company_profiles:
            data['company_profiles'] = company_profiles
        
        self.save_to_json(data, response.meta.get('original_url', response.url))
```

### CSS/XPath Selectors:
```
Selector: 'div.report_summary.full.non-indexable ul'
Element Navigation: 
  - Direct children: li.css(':scope > li')
  - Text extraction: li.css('strong::text, strong *::text').getall()
  - Direct text: li.xpath('text()').getall()
  - Child UL: li.css(':scope > ul')
```

### Step-by-Step Logic:
1. **Select TOC container**: CSS selector `div.report_summary.full.non-indexable ul`
2. **Iterate through LI elements** with enumeration (1-indexed)
3. **Extract text** from `<strong>` tags OR direct text nodes
4. **Build numbering**: Concatenate prefix levels with dots (1.1.2)
5. **Clean entry name**: Remove special characters, normalize
6. **Check if regional**: Skip if matches regional patterns
7. **Handle nesting**: Recursively process child UL elements
8. **Apply depth filter**: Only include sub-sub-segments if parent has ≥2 items
9. **Return numbered list** with all levels

### Filtering Rules:
```
✗ Skip if: is_regional_section(cleaned_name) = True
✗ Skip if: depth >= 2 AND word_count > 6
✗ Skip if: is_regional_line(line) = True (post-processing)
✓ Include: sub-sub-segments ONLY if parent LI has ≥2 children
```

### Output Format:
```
1. Market Segment A
1.1. Sub-segment A1
1.2. Sub-segment A2
1.2.1. Sub-sub-segment A2a (if ≥2 siblings)
1.2.2. Sub-sub-segment A2b (if ≥2 siblings)
2. Market Segment B
2.1. Sub-segment B1
```

---

## 2. MARKETS AND MARKETS - ACCORDION FORMAT

### Full Source Code (Lines 1365-1480)

```python
def parse_markets_accordion(self, response):
    toc_sections = []
    
    # STEP 1: Extract all accordion items
    for item in response.css("div.accordion-item"):
        main_title = item.css("div.TOCcustHead div:nth-child(1)::text").get()
        main_title = main_title.strip() if main_title else None
        sub_sections = []
        
        # STEP 2: Extract bullet items within each accordion
        for bullet_item in item.css("ul.toc_list li"):
            # Extract the main title (bulletsHead)
            main_title_elem = bullet_item.css("div.bulletsHead::text").get()
            if main_title_elem:
                main_title_text = main_title_elem.strip()
             
                # Extract all sub-sub-sections (bullets)
                sub_sub_sections = []
                for bullet in bullet_item.css("div.bullets::text"):
                    bullet_text = bullet.get().strip()
                    if bullet_text:
                        sub_sub_sections.append(bullet_text)
             
                sub_sections.append({
                    "title": main_title_text,
                    "sub_sub_sections": sub_sub_sections
                })
            else:
                # Handle regular list items without bulletsHead
                lines = [
                    remove_tags(t).strip()
                    for t in bullet_item.css("*::text").getall()
                    if remove_tags(t).strip()
                ]
                if lines:
                    sub_sections.append({
                        "title": lines[0],
                        "sub_sub_sections": []
                    })
        
        toc_sections.append({
            "main_title": main_title,
            "sub_sections": sub_sections,
        })
    
    # STEP 3: Format TOC with filtering and numbering
    formatted_toc = []
    chapter_counter = 0
    
    for chapter in toc_sections:
        if not chapter["main_title"]:
            continue
        
        # Remove numbering prefix (e.g., "1. Market Segmentation")
        chapter_title = remove_tags(re.sub(r"^\d+(\.\d+)*\s*", "", chapter["main_title"])).strip()
        
        # FILTERING: skip if main segment is only explanatory text in ()
        if re.fullmatch(r"\(.*\)", chapter_title):
            continue
        
        # FILTERING: Check stop keywords
        stop_keywords = ["BY REGION", "BY COUNTRY", "BY GEOGRAPHICAL"]
        if "BY" not in chapter_title.upper() or any(keyword in chapter_title.upper() for keyword in stop_keywords):
            continue
        
        # Extract "BY XXX" pattern
        match = re.search(r"BY.*", chapter_title, re.IGNORECASE)
        if not match:
            continue
        
        # Normalize: extract text after "BY"
        chapter_title = match.group(0).split("BY", 1)[1].strip().upper()
        
        # Renumber chapters
        chapter_counter += 1
        formatted_toc.append(f"{chapter_counter}. {chapter_title}")
        
        # STEP 4: Extract sub-sections
        section_counter = 1
        for sub in chapter.get("sub_sections", []):
            title = remove_tags(re.sub(r"^\d+(\.\d+)*\s*", "", sub.get("title", "").strip()))
            
            # FILTERING: Skip invalid entries
            if (
                not title
                or len(title.split()) > 6  # More than 6 words
                or re.search(r"\b(primary insights|key primary insights)\b", title.lower())
                or "introduction" in title.lower()
            ):
                continue

            formatted_toc.append(f"{chapter_counter}.{section_counter}. {title}")
            
            # STEP 5: Add sub-sub-sections (only if ≥2 items)
            sub_sub_sections = sub.get("sub_sub_sections", [])
            if len(sub_sub_sections) >= 2:
                sub_sub_counter = 1
                for sub_sub in sub_sub_sections:
                    if (
                        not sub_sub
                        or len(sub_sub.split()) > 6
                        or re.search(r"\b(primary insights|key primary insights)\b", sub_sub.lower())
                        or "introduction" in sub_sub.lower()
                    ):
                        continue

                    formatted_toc.append(f"{chapter_counter}.{section_counter}.{sub_sub_counter}. {sub_sub}")
                    sub_sub_counter += 1
            
            section_counter += 1

    # STEP 6: Extract company profiles
    company_profiles = []
    company_section = response.xpath(
        "//div[contains(@class,'TOCcustHead')][.//text()[contains(., 'COMPANY PROFILES')]]"
        "/following-sibling::div[contains(@class,'accordion-item-body')][1]"
    )

    inside_key_players = False
    for li in company_section.css("ul.toc_list > li"):
        head = li.css("div.bulletsHead::text").get()
        head_clean = head.strip().upper() if head else ""
        
        # Detect KEY / MAJOR PLAYERS
        if "KEY PLAYERS" in head_clean or "MAJOR PLAYERS" in head_clean:
            inside_key_players = True
        # Stop when another section starts
        elif head_clean and inside_key_players:
            break
        
        # Collect companies
        if inside_key_players:
            for company in li.css("div.bullets::text").getall():
                company = company.strip()
                if company and company.isupper():
                    company_profiles.append(company)

    return formatted_toc, company_profiles
```

### CSS/XPath Selectors:
```
Main selector: div.accordion-item
Title selector: div.TOCcustHead div:nth-child(1)::text
Bullet items: ul.toc_list li
Bullet head: div.bulletsHead::text
Sub-bullets: div.bullets::text

Company section XPath:
//div[contains(@class,'TOCcustHead')][.//text()[contains(., 'COMPANY PROFILES')]]
/following-sibling::div[contains(@class,'accordion-item-body')][1]

Company profiles: div.bullets::text
```

### Step-by-Step Logic:
1. **Select accordion items**: CSS `div.accordion-item`
2. **Extract main title**: `div.TOCcustHead div:nth-child(1)::text`
3. **Iterate through bullet items**: `ul.toc_list li`
4. **Get bullet head text**: `div.bulletsHead::text`
5. **Collect bullets**: `div.bullets::text` into array
6. **Filter chapter**: Check for "BY" keyword pattern
7. **Extract "BY XXX"**: Use regex to find pattern after "BY"
8. **Normalize to uppercase**: Convert segment type to UPPER
9. **Renumber sequentially**: Increment chapter_counter
10. **Process sub-sections**: Same filtering as chapters
11. **Include sub-sub only if ≥2 items**: Check array length
12. **Extract companies**: Find "COMPANY PROFILES" section

### Filtering Rules:
```
✗ Skip chapter if: "BY REGION" OR "BY COUNTRY" OR "BY GEOGRAPHICAL"
✗ Skip chapter if: Doesn't contain "BY" keyword
✗ Skip chapter if: Matches pattern r"\(.*\)" (only parentheses)
✗ Skip sub-section if: Empty OR >6 words OR contains "primary insights" OR "introduction"
✗ Skip sub-sub-section if: <2 items in array OR same filtering as sub-section
✓ Include companies: All text under "KEY PLAYERS" or "MAJOR PLAYERS"
```

### Output Format:
```
1. PRODUCT TYPE
1.1. Product A
1.2. Product B
1.3. Product C
1.3.1. Variant A (if ≥2 sub-subs)
1.3.2. Variant B (if ≥2 sub-subs)
2. APPLICATION
2.1. App 1
2.2. App 2
```

---

## 3. SNS INSIDER

### Full Source Code (Lines 1889-1980)

```python
def parse_sns(self, response):
    # Main page info
    title = self.clean_title(response.css('title::text').get() or "Untitled Report")
    main_url = response.url
    company_profiles = self.extract_sns_company_profiles(response)
    segmentation_url = f"{main_url.rstrip('/')}/segmentation"

    # Request segmentation page
    yield scrapy.Request(
        segmentation_url,
        callback=self.parse_sns_segmentation,
        meta={
            "title": title,
            "main_url": main_url,
            "company_profiles": company_profiles
        }
    )

def parse_sns_segmentation(self, response):
    # Get main page data from meta
    title = response.meta["title"]
    main_url = response.meta["main_url"]
    company_profiles = response.meta["company_profiles"]
    segmentation_data = self.extract_sns_segmentation_data(response)

    self.save_to_json({
        "title": title,
        "url": main_url,
        "table_of_contents": segmentation_data,
        "company_profiles": company_profiles
    }, main_url)

def extract_sns_segmentation_data(self, response):
    """Extract segmentation data from div.tab-content in flat TOC format, stopping at Regional Coverage."""
    toc = []
    segment_counter = 0
    
    # STEP 1: Select tab content container
    for tab_content in response.css("div.tab-content"):
        # STEP 2: Iterate through all P tags
        p_tags = tab_content.css("p")
        for p in p_tags:
            p_text = p.xpath("string()").get()
            if not p_text:
                continue
            p_text = p_text.strip()
            
            # FILTERING: Stop completely at "Regional Coverage"
            if "Regional Coverage" in p_text:
                return toc
            
            # FILTERING: Only consider main segments starting with "By"
            if not p_text.lower().startswith("by "):
                continue
            
            # STEP 3: Increment main segment counter
            segment_counter += 1
            
            # STEP 4: Remove "By" from title and uppercase
            segment_title = re.sub(r'^\s*by\s+', '', p_text, flags=re.IGNORECASE).strip()
            toc.append(f"{segment_counter}. {segment_title.upper()}")
            
            # STEP 5: Collect subsegments from following siblings
            sub_counter = 0
            siblings = p.xpath("following-sibling::*")
            
            for sibling in siblings:
                # FILTERING: Stop if "Regional Coverage" appears
                sib_text = sibling.xpath("string()").get()
                if sib_text and "Regional Coverage" in sib_text:
                    return toc

                tag_name = sibling.root.tag
                
                # STEP 6: Extract from UL elements
                if tag_name == "ul":
                    # Get all LI text
                    lis = sibling.css("li *::text, li::text").getall()
                    for li in lis:
                        li_text = li.strip()
                        if li_text:
                            sub_counter += 1
                            toc.append(f"{segment_counter}.{sub_counter}. {li_text}")
                
                # STEP 7: Stop at next "By" paragraph
                elif tag_name == "p":
                    if sib_text and sib_text.lower().startswith("by "):
                        break
    
    return toc
```

### CSS/XPath Selectors:
```
Tab content: div.tab-content
Paragraphs: p (within tab-content)
List items: li (within following UL)
Text extraction: li *::text, li::text
Following siblings: following-sibling::*
Text content: xpath("string()") - gets all text content
```

### Step-by-Step Logic:
1. **Parse main page**: Extract title and company profiles
2. **Build segmentation URL**: Append `/segmentation` to base URL
3. **Make separate request** to segmentation page
4. **In segmentation callback**:
   - Select all `div.tab-content` containers
   - Iterate through `p` tags
   - Extract text with `xpath("string()")`
5. **Check for STOP keyword**: "Regional Coverage"
6. **Filter for "By" pattern**: Starts with "By " (case-insensitive)
7. **Extract segment name**: Remove "By " prefix, uppercase
8. **Collect subsegments**: Get following siblings
9. **Check for UL elements**: If `<ul>`, collect all LI text
10. **Stop at next segment**: If next P starts with "By", break loop

### Filtering Rules:
```
✗ Skip if: No text content found
✗ Stop if: "Regional Coverage" appears
✗ Skip if: P tag doesn't start with "By "
✗ Skip subsegment if: Empty or whitespace-only
✗ Stop subsegments if: Next P tag starts with "By "
✓ Include: All LI text content (with whitespace trimmed)
```

### Output Format:
```
1. PRODUCT TYPE
1.1. Product A
1.2. Product B
1.3. Product C
2. DISTRIBUTION CHANNEL
2.1. Online
2.2. Offline
2.3. Hybrid
```

---

## 4. MORDOR INTELLIGENCE

### Full Source Code (Lines 1995-2200)

```python
def parse_mordor(self, response):
    # STEP 1: Select TOC container
    toc_selector = response.css("#table-of-content")
    if not toc_selector:
        self.logger.warning(f"No TOC found on {response.url}")
        self.save_to_json({
            'title': self.clean_title(response.css('title::text').get()),
            'url': response.url,
            'table_of_contents': [],
            'company_profiles': []
        }, response.url)
        return
    
    # STEP 2: Extract all text content from TOC
    toc_text = toc_selector.xpath('normalize-space(.)').get()
    
    # STEP 3: Find all segmentation patterns that start with "By"
    segmentation_data = self.extract_mordor_segmentation_data(toc_text)
    
    # STEP 4: Normalize the numbering
    normalized_data = self.normalize_mordor_numbering(segmentation_data)
    
    # STEP 5: Extract company profiles
    company_profiles = self.extract_mordor_company_profiles(response)
    
    self.save_to_json({
        'title': self.clean_title(response.css('title::text').get()),
        'url': response.url,
        'table_of_contents': normalized_data,
        'company_profiles': company_profiles
    }, response.url)

def extract_mordor_segmentation_data(self, toc_text):
    """Extract segments using regex patterns"""
    segmentation_lines = []
    
    # FILTERING: Split at "By Geography" to remove regional sections
    toc_text = re.split(r'\d+\.\d+\s+By\s+Geography', toc_text)[0]
    
    # STEP 1: Find all main patterns: "5.1 By Product Type"
    main_pattern = r'(\d+\.\d+\s+By\s+[A-Za-z\s&/\-]+)'
    main_matches = re.findall(main_pattern, toc_text)
    
    # STEP 2: Iterate through main matches
    for main_match in main_matches:
        # Normalize whitespace
        main_header = re.sub(r'\s+', ' ', main_match.strip())

        # Extract base number (e.g., "5.1")
        base_number_match = re.search(r'^(\d+\.\d+)', main_header)
        if not base_number_match:
            continue

        base_number = base_number_match.group(1)
        segmentation_lines.append(main_header)

        # STEP 3: Find sub-patterns using base number
        # Pattern: "5.1.1 Product Name", "5.1.2 Another Product"
        sub_pattern = rf'({re.escape(base_number)}\.\d+(?:\.\d+)*\s+[A-Za-z\s&/\-]+)'
        sub_matches = re.findall(sub_pattern, toc_text)

        # STEP 4: Add all sub-matches with leading space
        for sub_match in sub_matches:
            segmentation_lines.append(" " + re.sub(r'\s+', ' ', sub_match.strip()))

    return segmentation_lines

def normalize_mordor_numbering(self, segmentation_data):
    """Normalize the numbering system to create proper hierarchical structure"""
    normalized_data = []
    main_counter = 0
    sub_counter = 0
    sub_sub_counter = 0
    last_sub_item = None
    last_sub_sub_item = None
    
    # STEP 1: Iterate through all lines
    for line in segmentation_data:
        # STEP 2: Check if it's a main header (no leading spaces)
        if not line.startswith(' '):
            main_counter += 1
            sub_counter = 0
            sub_sub_counter = 0
            last_sub_item = None
            last_sub_sub_item = None
            
            # STEP 3: Extract the text after "By"
            if "By " in line:
                # Extract everything after "By "
                text_part = line.split("By ", 1)[1].strip()
                # Remove any numbering prefix that might be left
                text_part = re.sub(r'^\d+\.\d+\s*', '', text_part).strip()
            else:
                # If "By" is not found, use original text after numbering
                text_part = re.sub(r'^\d+\.\d+\s+', '', line).strip()
            
            # Create new numbering for main header
            new_line = f"{main_counter}. {text_part}"
            normalized_data.append(new_line)
        
        else:
            # STEP 4: It's a sub-item - parse the old numbering
            clean_line = line.strip()
            
            # Extract old numbering pattern
            old_number_match = re.match(r'^(\d+\.\d+(?:\.\d+)*)\s+(.+)', clean_line)
            
            if old_number_match:
                old_number = old_number_match.group(1)
                text_part = old_number_match.group(2).strip()
                
                # Count the dots in the old number to determine level
                dot_count = old_number.count('.')
                parts = old_number.split('.')
                
                if dot_count == 2:
                    # Level 1 sub-item (e.g., "5.1.1") - becomes 1.1, 1.2, 1.3
                    current_sub_item = f"{parts[0]}.{parts[1]}.{parts[2]}"
                    
                    # If this is a new level-1 item number, increment sub_counter
                    if current_sub_item != last_sub_item:
                        sub_counter += 1
                        sub_sub_counter = 0
                        last_sub_item = current_sub_item
                    
                    new_line = f" {main_counter}.{sub_counter}. {text_part}"
                
                elif dot_count == 3:
                    # Level 2 sub-item (e.g., "5.1.1.1") - becomes 1.1.1
                    current_sub_sub_item = old_number
                    
                    # If this is a new level-2 item, increment sub_sub_counter
                    if current_sub_sub_item != last_sub_sub_item:
                        sub_sub_counter += 1
                        last_sub_sub_item = current_sub_sub_item
                    
                    new_line = f" {main_counter}.{sub_counter}.{sub_sub_counter}. {text_part}"
                
                else:
                    # For deeper or other nesting
                    new_number = f"{main_counter}." + ".".join(parts[2:])
                    new_line = f" {new_number}. {text_part}"
                
                normalized_data.append(new_line)
            else:
                # Fallback: just clean and add
                text_part = re.sub(r'^\d+(?:\.\d+)*\s*', '', clean_line).strip()
                sub_counter += 1
                normalized_data.append(f" {main_counter}.{sub_counter}. {text_part}")
    
    return normalized_data
```

### CSS/XPath Selectors:
```
TOC selector: #table-of-content
Text extraction: xpath('normalize-space(.)') - all text as single string
Main pattern regex: r'(\d+\.\d+\s+By\s+[A-Za-z\s&/\-]+)'
Sub pattern regex: r'({base_number}\.\d+(?:\.\d+)*\s+[A-Za-z\s&/\-]+)'
```

### Step-by-Step Logic:
1. **Select TOC**: `#table-of-content`
2. **Get all text**: `xpath('normalize-space(.)') ` - single long string
3. **Remove regions**: Split at "By Geography"
4. **Extract main patterns**: Regex find all "5.1 By Product Type"
5. **For each main pattern**:
   - Extract base number (5.1)
   - Find all sub-patterns with that base
   - Add leading space to sub-items
6. **Normalize numbering**:
   - Check for leading spaces (indicates level)
   - Extract old numbering (5.1.1, 5.1.1.1, etc.)
   - Count dots to determine hierarchy
   - Renumber sequentially
7. **Transform**: Old "5.1.1" → New "1.1"

### Filtering Rules:
```
✗ Remove: Everything after "By Geography"
✗ Skip: Main items that don't match pattern
✗ Skip: Sub-items without valid numbering
✓ Include: All matched patterns
Normalization: Converts original numbers to sequential 1, 1.1, 1.1.1
```

### Output Format:
```
1. Product Type
 1.1. Product A
 1.2. Product B
 1.2.1. Variant A (if exists)
 1.2.2. Variant B (if exists)
2. Application Type
 2.1. Application 1
```

---

## 5. FORTUNE BUSINESS INSIGHTS

### Full Source Code (Lines 2255-2480)

```python
def parse_fortune_main(self, response):
    title = self.clean_title(response.css('title::text').get())
    main_url = response.url
    company_profiles = self.extract_company_profiles(response)

    # Build segmentation URL
    if "/industry-reports/" in main_url:
        segmentation_url = main_url.replace(
            "/industry-reports/",
            "/industry-reports/segmentation/"
        )
    else:
        segmentation_url = main_url.replace(
            "fortunebusinessinsights.com/",
            "fortunebusinessinsights.com/segmentation/"
        )

    # Request the segmentation page
    yield scrapy.Request(
        segmentation_url,
        callback=self.parse_fortune_segmentation,
        meta={
            "title": title,
            "main_url": main_url,
            "company_profiles": company_profiles,
            "is_main_page": False,
            "zyte_api_automap": True,
            "zyte_api_render": {
                "browserHtml": True,
                "waitFor": "div.tab-content"
            }
        }
    )

def parse_fortune_segmentation(self, response):
    structured_points = []
    main_url = response.meta["main_url"]
    title = response.meta["title"]
    company_profiles = response.meta.get("company_profiles", [])
    
    # STEP 1: Select all table rows
    rows = response.xpath('//div[@id="industrycoverage"]//table//tr')
    main_count = 0
    start_processing = False
    
    # STEP 2: Iterate through each row
    for row in rows:
        # STEP 3: Decide which TD contains the "By" heading
        if row.xpath('./td[@rowspan]'):
            left_td = row.xpath('./td[2]')
        elif start_processing and not row.xpath('./td[2]'):
            left_td = row.xpath('./td[1]')
        else:
            left_td = row.xpath('./td[1]')
        
        # STEP 4: Extract "By" text from strong tag or p tag
        left_text = ' '.join(
            t.strip()
            for t in left_td.xpath('.//p//strong//text()').getall()
            if t.strip()
        )
        
        # FALLBACK: if strong tag missing
        if not left_text:
            left_text = ' '.join(
                t.strip()
                for t in left_td.xpath('./p[1]//text()').getall()
                if t.strip()
            )
        
        # STEP 5: Detect start of segmentation
        seg_text = ' '.join(
            t.strip() for t in row.xpath('./td[1]//text()').getall() if t.strip()
        )
        if not start_processing:
            if "segmentation" in seg_text.lower():
                start_processing = True
            else:
                continue

        # STEP 6: Skip rows not starting with "By"
        if not left_text.lower().startswith("by"):
            if not start_processing:
                continue

            # ALTERNATIVE: Check for "By" in content TD
            content_td = row.xpath('./td[2]')
            by_heads = content_td.xpath(
                './/p/strong[starts-with(normalize-space(.), "By")]/text()'
            ).getall()
            if not by_heads:
                continue

            for head in by_heads:
                lower_head = head.lower()
                # FILTERING: Skip region/geography/country
                if any(k in lower_head for k in ("by region", "by geography", "by country")):
                    continue
                
                main_count += 1
                sub = sub_sub = sub_sub_sub = 0
                segment = re.sub(r'^by\s+', '', head, flags=re.IGNORECASE).strip()
                structured_points.append(f"{main_count}. {segment}")
                
                # Get UL after the heading
                ul = content_td.xpath(f'.//p[strong[text()="{head}"]]/following-sibling::ul[1]')
                for li in ul.xpath('.//li'):
                    text = ' '.join(li.xpath('.//text()').getall()).replace('\xa0', ' ').strip()
                    if text:
                        sub += 1
                        structured_points.append(f"{main_count}.{sub}. {text}")
            continue
        
        # STEP 7: Skip region/geography/country in main heading
        lower_left = left_text.lower()
        if any(k in lower_left for k in ("by region", "by geography", "by country")):
            continue
        
        # STEP 8: Main segment found - add with numbering
        main_count += 1
        sub = sub_sub = sub_sub_sub = 0
        segment = re.sub(r'^by\s+', '', left_text, flags=re.IGNORECASE).strip()
        structured_points.append(f"{main_count}. {segment}")
        
        # STEP 9: Get content TD (either TD2 or TD1)
        content_td = row.xpath('./td[2]') if row.xpath('./td[2]') else row.xpath('./td[1]')
        found_symbol_items = False
        
        # STEP 10: Parse content with symbol-based hierarchy
        for p in content_td.xpath('.//p'):
            text = ''.join(p.xpath('.//text()').getall())
            text = text.replace('\xa0', ' ').strip()
            if not text:
                continue
            
            # LEVEL 2 — · (bullet symbol)
            if text.startswith('·'):
                sub += 1
                sub_sub = sub_sub_sub = 0
                structured_points.append(
                    f"{main_count}.{sub}. {text.lstrip('·').strip()}"
                )
                found_symbol_items = True
            
            # LEVEL 3 — o (circle)
            elif text.startswith('o'):
                clean_text = text.lstrip('o').strip()
                if len(clean_text.split()) <= 6:
                    sub_sub += 1
                    sub_sub_sub = 0
                    structured_points.append(
                        f"{main_count}.{sub}.{sub_sub}. {clean_text}"
                    )
                    found_symbol_items = True
            
            # LEVEL 4 — § (section)
            elif text.startswith('§'):
                clean_text = text.lstrip('§').strip()
                if len(clean_text.split()) <= 6:
                    sub_sub_sub += 1
                    structured_points.append(
                        f"{main_count}.{sub}.{sub_sub}.{sub_sub_sub}. {clean_text}"
                    )
                    found_symbol_items = True
        
        # STEP 11: Fallback to UL/LI if no symbols found
        if not found_symbol_items:
            for li in content_td.xpath('.//li'):
                text = ' '.join(li.xpath('.//text()').getall()).strip()
                if text:
                    sub += 1
                    structured_points.append(f"{main_count}.{sub}. {text}")

    result_data = {
        "title": title,
        "url": main_url,
        "table_of_contents": structured_points
    }
    if company_profiles:
        result_data["company_profiles"] = company_profiles
    
    self.save_to_json(result_data, main_url)
```

### CSS/XPath Selectors:
```
Table rows: //div[@id="industrycoverage"]//table//tr
Left column: td[1] or td[2] (depends on rowspan)
Strong text: .//p//strong//text()
Paragraph text: ./p[1]//text()
Strong by heading: .//p/strong[starts-with(normalize-space(.), "By")]/text()
Content TD: td[2] (right column)
List items: .//li
Symbol patterns: text.startswith('·'), text.startswith('o'), text.startswith('§')
```

### Step-by-Step Logic:
1. **Select table**: `//div[@id="industrycoverage"]//table//tr`
2. **Iterate rows**: For each row in table
3. **Determine left column**: Check for rowspan to find "By" heading
4. **Extract "By" text**: From `strong` tag in `p` or fallback
5. **Check for "segmentation"**: Detect start of content
6. **Filter by keyword**: Skip "BY REGION", "BY COUNTRY", etc.
7. **Extract segment name**: Remove "By " prefix
8. **Number and add**: `main_count.`
9. **Get content column**: Right TD
10. **Parse by symbols**:
    - `·` = Level 2 (sub-segment)
    - `o` = Level 3 (sub-sub)
    - `§` = Level 4 (sub-sub-sub)
11. **Increment counter** based on symbol level

### Filtering Rules:
```
✗ Skip if: Doesn't start with "by"
✗ Skip if: Contains "by region", "by geography", "by country"
✗ Skip if: Word count > 6 (for levels 3 and 4)
✗ Skip if: Empty or whitespace only
✓ Include: All levels with correct symbols
Fallback: Use UL/LI if no symbols found
```

### Output Format:
```
1. PRODUCT TYPE
1.1. Product A
1.2. Product B
1.2.1. Variant A
1.2.1.1. Sub-variant A1
2. TECHNOLOGY
2.1. AI-based
2.2. Cloud-based
```

---

## 6. FUTURE MARKET INSIGHTS

### Full Source Code (Lines 2484-2640)

```python
def parse_future(self, response):
    # Extract market segmentation data and format as table_of_contents
    table_of_contents = self.extract_future_segmentation_as_toc(response)
    # Extract company profiles
    company_profiles = self.extract_future_company_profiles(response)
    
    result_data = {
        'title': self.clean_title(response.css('title::text').get()),
        'url': response.url,
        'table_of_contents': table_of_contents,
        'company_profiles': company_profiles
    }
    self.save_to_json(result_data, response.url)

def extract_future_segmentation_as_toc(self, response):
    """Extract ONLY market segmentation data and format as numbered TOC"""
    toc_lines = []
    content_div = response.css('div.report_content_div div.tab_content')
    added_segments = set()
    
    # STEP 1: Find Segmentation H2 heading
    segmentation_h2 = None
    for h2 in content_div.css('h2'):
        text = h2.xpath('normalize-space(.)').get()
        if not text:
            continue
        
        text_lower = text.lower()
        
        # FILTERING: Skip if contains "segmental analysis"
        if 'segmental analysis' in text_lower:
            continue
        
        # Check for segmentation keywords
        if (
            any(word in text_lower for word in ['segment', 'segments', 'key segments'])
            and '?' not in text_lower
        ):
            segmentation_h2 = h2
            break

    if not segmentation_h2:
        return toc_lines
    
    # STEP 2: Collect all nodes until next H2
    nodes = []
    for sib in segmentation_h2.xpath('following-sibling::*'):
        if sib.root.tag == 'h2':
            break
        nodes.append(sib)
    
    # STEP 3: Parse segments
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
            
            # Remove "By " prefix
            if title.lower().startswith('by '):
                title = title[3:].strip()
            
            title_clean = title.lower()
            
            # FILTERING: Skip geographic sections
            if title_clean in {'region', 'regions', 'country', 'countries'}:
                current_category = None
                continue
            
            # FILTERING: Skip if already added
            if title_clean in added_segments:
                current_category = None
                continue
            
            current_category = title
        
        # SUB SEGMENTS (UL)
        elif tag == 'ul' and current_category:
            sub_num = 1
            sub_lines = []
            first_level_has_valid_items = False
            
            # STEP 4: Iterate through LI items
            for li in node.xpath('./li'):
                li_text = li.xpath('normalize-space(text())').get()
                if not li_text:
                    continue
                li_text = li_text.strip()
                
                # STEP 5: Check if this LI has a nested UL
                nested_ul = li.xpath('./ul')
                if nested_ul:
                    nested_items = []
                    for nested_li in nested_ul.xpath('./li'):
                        nested_text = nested_li.xpath('normalize-space(.)').get()
                        if nested_text:
                            nested_items.append(nested_text.strip())
                    
                    # FILTERING: Only add if ≥2 nested items
                    if len(nested_items) >= 2:
                        sub_lines.append(f"{main_num}.{sub_num}. {li_text}")
                        
                        sub_sub_num = 1
                        for nested_text in nested_items:
                            sub_lines.append(f"{main_num}.{sub_num}.{sub_sub_num}. {nested_text}")
                            sub_sub_num += 1
                        
                        first_level_has_valid_items = True
                
                else:
                    # No nested UL, treat li normally as sub-segment
                    sub_lines.append(f"{main_num}.{sub_num}. {li_text}")
                    first_level_has_valid_items = True
                
                sub_num += 1
            
            # STEP 6: Only add category if has valid sub-items
            if first_level_has_valid_items:
                toc_lines.append(f"{main_num}. {current_category}")
                added_segments.add(current_category.lower())
                toc_lines.extend(sub_lines)
                main_num += 1
            
            current_category = None
    
    return toc_lines
```

### CSS/XPath Selectors:
```
Content container: div.report_content_div div.tab_content
H2 headings: h2
Text in H2: normalize-space(.)
Following siblings: following-sibling::*
H3 headings: h3
LI items: ./li
Nested UL: ./ul
Nested LI items: ./li
Text extraction: normalize-space(.) and normalize-space(text())
```

### Step-by-Step Logic:
1. **Find content div**: `div.report_content_div div.tab_content`
2. **Search for Segmentation H2**: Loop through all H2 tags
3. **Check H2 text**: Must contain "segment(s)" or "key segments"
4. **Skip keywords**: Avoid "segmental analysis"
5. **Collect nodes**: Get all siblings until next H2
6. **Parse nodes**:
   - If tag is H3: Extract as category
   - If tag is UL: Extract as sub-segments
7. **For H3 (category)**:
   - Remove "By " prefix
   - Check if category is region (skip if yes)
   - Check if already added (skip if yes)
   - Set as current_category
8. **For UL (sub-segments)**:
   - Iterate through LI items
   - Check if LI has nested UL
   - If nested UL has ≥2 items, include all levels
   - If no nested UL, treat as single sub-segment
   - Add category only if ≥1 valid sub-item

### Filtering Rules:
```
✗ Skip H2 if: Contains "segmental analysis"
✗ Skip H3 if: Text is "region", "regions", "country", "countries"
✗ Skip H3 if: Already in added_segments
✗ Skip nested UL if: <2 items in nested list
✗ Skip LI if: Empty or whitespace only
✓ Include: Category only if ≥1 valid sub-segment
```

### Output Format:
```
1. Product Type
1.1. Product A
1.2. Product B
1.2.1. Sub-variant 1 (if nested ≥2)
1.2.2. Sub-variant 2 (if nested ≥2)
2. Distribution Channel
2.1. Online
2.2. Retail
```

---

## 7. ALLIED MARKET RESEARCH

### Full Source Code (Lines 2640-2870)

```python
def parse_allied(self, response):
    report_id = response.css('input#report_id::attr(value)').get()
    if not report_id:
        script_content = response.xpath('//script[contains(text(), "report_id")]/text()').get()
        if script_content and 'report_id' in script_content:
            report_id = script_content.split('report_id:')[1].split(',')[0].strip().strip("'\"")
    
    if report_id:
        toc_url = f"https://www.alliedmarketresearch.com/get-report-toc-rev/{report_id}"
        yield scrapy.Request(
            toc_url,
            callback=self.parse_allied_toc,
            meta={
                "page_url": response.url,
                "title": response.css('title::text').get()
            }
        )
    else:
        # Fallback if no report ID
        full_title = response.css('title::text').get()
        market_name = self.extract_market_name(full_title)
        self.save_to_json({
            'title': market_name,
            'url': response.url,
            'table_of_contents': ["❌ Report ID not found"],
            'company_profiles': []
        }, response.url)

def parse_allied_toc(self, response):
    flat_toc_list = []
    company_profiles_list = []
 
    chapter_number = 0
    start_collecting = False
    stop_collecting = False
    in_company_profiles = False
    
    # STEP 1: Iterate through all accordion cards
    for chapter_card in response.css('#acordTabOCnt > .card'):
        # STEP 2: Extract chapter title
        chapter_title = chapter_card.css('.card-header .btn-link .fw-700::text').get()
        if chapter_title:
            # Remove numbering prefix (e.g., "1. ")
            chapter_title = re.sub(r'^\d+(\.\d+)*\.\s*', '', chapter_title.strip())
         
            # FILTERING: Check if in company profiles section
            if "company profile" in chapter_title.lower() or "company profiles" in chapter_title.lower():
                in_company_profiles = True
                stop_collecting = True  # Stop collecting segmentation
            
            elif in_company_profiles:
                # We've passed the company profiles section
                break
         
            # FILTERING: Start collecting after "Market Overview"
            if not start_collecting and not in_company_profiles:
                if "market overview" in chapter_title.lower() or "market landscape" in chapter_title.lower():
                    start_collecting = True
                    continue  # Skip market overview chapter itself
                else:
                    continue
            
            # FILTERING: Check for "BY REGION" chapter and stop collecting
            if not in_company_profiles and " BY REGION" in chapter_title.upper():
                stop_collecting = True
                continue
            
            # STEP 3: Extract segmentation sections
            if not in_company_profiles and not stop_collecting:
                # Format check 1: "MARKET NAME, BY SEGMENT_TYPE"
                if " BY " in chapter_title.upper():
                    segment_type = chapter_title.upper().split(" BY ")[-1].strip()
                    chapter_number += 1
                    flat_toc_list.append(f"{chapter_number}. {segment_type}")
                
                # Format check 2: "MARKET NAME, SEGMENT_TYPE"
                elif "," in chapter_title and ":" in chapter_title:
                    segment_part = chapter_title.split(",")[-1].strip()
                    chapter_number += 1
                    flat_toc_list.append(f"{chapter_number}. {segment_part}")
                
                else:
                    # Generic fallback
                    chapter_number += 1
                    flat_toc_list.append(f"{chapter_number}. {chapter_title}")
        
        # STEP 4: Process segmentation sections
        if not in_company_profiles and not stop_collecting:
            card_body = chapter_card.css('.card-body')
            if card_body:
                # STEP 5: Get all potential section elements
                main_sections = card_body.css('h3, p span span, p span, p')
                section_number = 1
                seen_subsegments = set()
                temp_subsegments = []
                
                # STEP 6: Collect all valid sub-segments
                for section in main_sections:
                    text = ' '.join(section.css('::text').getall()).strip()
                    if not text:
                        continue

                    # FILTERING: Skip 3rd+ level items
                    if re.match(r'^\d+\.\d+\.\d+\.', text):
                        continue

                    # Extract sub-segment number and text
                    match = re.match(r'^\d+\.\d+\.\s*(.+)', text)
                    if not match:
                        continue

                    clean_title = match.group(1).strip()

                    # FILTERING: Skip unwanted keywords
                    if re.search(
                        r'market size|forecast|key market|overview|opportunit|by region',
                        clean_title,
                        re.IGNORECASE
                    ):
                        continue

                    # FILTERING: Skip if more than 6 words
                    if len(clean_title.split()) > 6:
                        continue

                    # FILTERING: Skip duplicates
                    key = (chapter_number, clean_title.lower())
                    if key in seen_subsegments:
                        continue

                    seen_subsegments.add(key)
                    temp_subsegments.append(clean_title)
                
                # STEP 7: Only add sub-segments if count >= 2
                if len(temp_subsegments) >= 2:
                    for sub in temp_subsegments:
                        flat_toc_list.append(f"{chapter_number}.{section_number}. {sub}")
                        section_number += 1

        # STEP 8: Process company profiles
        if in_company_profiles:
            card_body = chapter_card.css('.card-body')
            if card_body:
                # STEP 9: Extract company names
                company_h3s = card_body.css('p, p span span, p span, h3')
                for item in company_h3s:
                    text = ' '.join(item.css('::text').getall()).strip()
                    if not text:
                        continue

                    # FILTERING: Only 2nd level items (X.X.)
                    match = re.match(r'^\d+\.\d+\.\s*(.+)', text)
                    if not match:
                        continue

                    company_name = match.group(1).strip()

                    # FILTERING: Remove junk headings
                    if re.search(
                        r'overview|definition|research|dynamics|analysis|description',
                        company_name,
                        re.IGNORECASE
                    ):
                        continue

                    # FILTERING: Clean suffixes
                    company_name = re.sub(r'\s*\(.*?\)', '', company_name)
                    company_name = re.sub(
                        r'\s*(Inc|Ltd|LLC|Corp|Co|Limited|Pvt)\.?$',
                        '',
                        company_name,
                        flags=re.IGNORECASE
                    ).strip()

                    if company_name and company_name not in company_profiles_list:
                        company_profiles_list.append(company_name)
    
    # STEP 10: Extract and save market name
    full_title = response.meta["title"]
    market_name = self.extract_market_name(full_title)
    
    self.save_to_json({
        'title': market_name,
        'url': response.meta["page_url"],
        'table_of_contents': flat_toc_list if flat_toc_list else ["No segmentation found"],
        'company_profiles': company_profiles_list if company_profiles_list else []
    }, response.meta["page_url"])
```

### CSS/XPath Selectors:
```
Report ID: input#report_id::attr(value)
Script text: //script[contains(text(), "report_id")]/text()
Accordion cards: #acordTabOCnt > .card
Card title: .card-header .btn-link .fw-700::text
Card body: .card-body
Section elements: h3, p span span, p span, p
All text in element: ::text
```

### Step-by-Step Logic:
1. **Extract report ID**: From input field or script tag
2. **Build TOC URL**: `https://www.alliedmarketresearch.com/get-report-toc-rev/{report_id}`
3. **Request TOC page**: Async request to TOC API
4. **Select all cards**: `#acordTabOCnt > .card`
5. **For each card**:
   - Extract chapter title
   - Remove numbering prefix
   - Check if company profiles section
   - Check if market overview/landscape
6. **Segment extraction**:
   - Skip if "BY REGION" appears
   - Extract segment type after "BY"
   - Increment chapter number
   - Add to flat TOC list
7. **Sub-segment extraction**:
   - Get card body elements
   - Find all P and H3 tags
   - Extract text and parse numbering
   - Collect into temp array
   - Add to TOC only if ≥2 items
8. **Company profile extraction**:
   - When in company profiles section
   - Extract 2nd-level items (X.X.)
   - Filter out headings
   - Clean company names
   - Add to profiles list

### Filtering Rules:
```
✗ Skip chapter if: Doesn't contain " BY " AND doesn't match comma format
✗ Stop chapter if: Contains " BY REGION"
✗ Skip sub-segment if: Starts with 3rd+ level (X.X.X.)
✗ Skip sub-segment if: >6 words
✗ Skip sub-segment if: Contains "market size", "forecast", "key market", "overview", "opportunity", "by region"
✗ Skip sub-segment if: Already added (duplicate)
✗ Skip company if: Contains "overview", "definition", "research", "dynamics", "analysis", "description"
✓ Include: Sub-segments only if ≥2 items found
✓ Include: Companies from company profiles section
```

### Output Format:
```
1. PRODUCT TYPE
1.1. Product A
1.2. Product B
1.3. Product C
2. DISTRIBUTION CHANNEL
2.1. Online
2.2. Retail
```

---

## Helper Functions

### `extract_segments_from_toc()` (Lines 73-108)

```python
def extract_segments_from_toc(table_of_contents):
    """Extract segments and sub-segments from table of contents.
    
    Converts:
    ['1. Segment Name', '1.1. Sub-segment 1', '1.2. Sub-segment 2']
    
    To:
    {'Segment Name': ['Sub-segment 1', 'Sub-segment 2']}
    """
    segments = {}
    current_segment = None
    
    if not isinstance(table_of_contents, list):
        return segments
    
    for line in table_of_contents:
        if not isinstance(line, str):
            continue
        
        # Parse "1. Name" or "1.1. Name" format
        match = re.match(r'^\s*(\d+(?:\.\d+)*)[.)]?\s+(.+)$', line.strip())
        if not match:
            continue
        
        numbering = match.group(1)
        text = match.group(2).strip().title()
        
        # Count dots to determine hierarchy level
        depth = numbering.count('.')
        
        if depth == 0:
            # Top-level segment
            current_segment = text
            if current_segment not in segments:
                segments[current_segment] = []
        
        elif depth == 1 and current_segment is not None:
            # Sub-segment (X.Y)
            segments[current_segment].append(text)
    
    # Keep only segments with sub-segments
    segments = {k: v for k, v in segments.items() if v}
    
    return segments
```

---

## Common Filtering Patterns

```python
# Regional keywords to skip
regional_patterns = [
    r'regional outlook.*volume.*revenue',
    r'.*regional outlook$',
    r'by region',
    r'by country',
    r'country outlook',
    r'Regional',
    # ... country names ...
]

# Common keywords to filter
stop_keywords = ["BY REGION", "BY COUNTRY", "BY GEOGRAPHICAL"]

# Word count threshold
MAX_WORDS = 6

# Minimum subsegments
MIN_SUBSEGMENTS = 2

# Stop phrases
stop_phrases = [
    "Regional Coverage",
    "Company Profiles",
    "Methodology",
    "Appendix"
]
```

---

## Summary

| Parser | Selector | Levels | Filter | Output |
|--------|----------|--------|--------|--------|
| **Grand View** | CSS `.report_summary ul` | 3 | Regional, >6 words, ≥2 items | 1.1.1 format |
| **M&M Accordion** | CSS `.accordion-item` | 3 | "BY" keyword, stop keywords | 1.1.1 format |
| **SNS** | CSS `p` + `ul` | 2 | "By" prefix, "Regional Coverage" | 1.1 format |
| **Mordor** | Regex `\d+\.\d+\s+By` | 3 | "By Geography", renumbered | 1.1.1 format |
| **Fortune** | XPath table + symbols | 4 | "By region/geography", >6 words | 1.1.1.1 format |
| **Future** | CSS H2+H3+UL | 3 | Region names, duplicates, ≥2 nested | 1.1.1 format |
| **Allied** | API cards | 2 | Region, ≥2 items | 1.1 format |

