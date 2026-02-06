import io
import os
from PIL import Image, ImageDraw, ImageFont
import google.genai as genai
from dotenv import load_dotenv
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TEMPLATE_PATH = "temp2.png"
OUTPUT_SIZE = (500, 600)
FONT_PATH_POPPINS = "Poppins-Bold.ttf"
FONT_PATH_FALLBACK = "/System/Library/Fonts/Helvetica.ttc"
MARKET_TEXT_POSITION = (25, 70)
MARKET_TEXT_MAX_WIDTH = 440
MARKET_TEXT_FONT_SIZE = 30
 
# EXACT coordinates of the Image box
IMAGE_BOX = (16, 307, 484, 586)
 
def load_font(size, font_path=None):
    font_paths = []
   
    if font_path:
        font_paths.append(font_path)
   
    font_paths.extend([
        FONT_PATH_POPPINS,
        "Poppins-Bold.ttf",
        "/usr/share/fonts/truetype/poppins/Poppins-Bold.ttf",
        "Poppins-Regular.ttf",
        "/usr/share/fonts/truetype/poppins/Poppins-Regular.ttf",
        "Poppins.ttf",
        FONT_PATH_FALLBACK,
        # macOS font paths
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        # Linux paths
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ])
   
    for fp in font_paths:
        try:
            if os.path.exists(fp):
                font = ImageFont.truetype(fp, size)
                print(f"Loaded font: {fp} at size {size}")
                return font
        except Exception:
            continue
   
    print(f"WARNING: Could not find any TrueType fonts. Text will be very small!")
    print(f"Please install a font or provide correct font path.")
    return ImageFont.load_default()
 
def genai_image_to_pil(genai_image):
    if isinstance(genai_image, Image.Image):
        return genai_image
    image_bytes = getattr(genai_image, "image_bytes", None)
    if image_bytes:
        return Image.open(io.BytesIO(image_bytes)).convert("RGB")
    pil_img = getattr(genai_image, "_pil_image", None)
    if pil_img:
        return pil_img
    raise TypeError("Unsupported Gemini image type")
 
def compress_webp_under_target(image: Image.Image, output_path: str, target_kb: int = 18):
    """
    Compress PIL Image to WebP under target_kb, keeping dimensions.
    Returns final quality used and actual size.
    """
    quality = 90
    while quality >= 20:
        buffer = io.BytesIO()
        image.save(buffer, format="WEBP", quality=quality, method=6)
        size_kb = buffer.tell() / 1024
        if size_kb <= target_kb:
            with open(output_path, "wb") as f:
                f.write(buffer.getvalue())
            return quality, size_kb
        quality -= 5
   
    image.save(output_path, format="WEBP", quality=20, method=6)
    size_kb = os.path.getsize(output_path) / 1024
    return 20, size_kb
 
def wrap_text(draw, text, font, max_width):
    """
    Wrap text into multiple lines based on rendered width.
    Line breaks occur only at word boundaries.
    """
    words = text.split()
    lines = []
    current_line = ""
 
    for word in words:
        test_line = word if not current_line else current_line + " " + word
       
        bbox = draw.textbbox((0, 0), test_line, font=font)
        text_width = bbox[2] - bbox[0]
 
        if text_width <= max_width:
            current_line = test_line
        else:
            lines.append(current_line)
            current_line = word
 
    if current_line:
        lines.append(current_line)
 
    return lines
 
def force_two_lines(text):
    """
    Force text into exactly two balanced lines by word count.
    No words are dropped.
    """
    words = text.split()
    mid = len(words) // 2
    return [
        " ".join(words[:mid]),
        " ".join(words[mid:])
    ]
 
def generate_scene_prompt_with_llm(market_name: str):
    """
    Gemini 2.5 Flash (Vertex AI):
    Converts a market name into ONE short, concrete, real-life scene sentence.
    """
    client = genai.Client(vertexai=True, api_key=GEMINI_API_KEY)
 
    prompt = (
    f"Generate 1 short, simple, easy to understand sentences written as an image command for an image generation model. "
    f"Describe a realistic photograph where the primary subject is the physical products or equipment of the {market_name}, "
    "clearly visible and arranged in a real-world usage environment appropriate to the market. "
    "Include at least one concrete material or physical detail to emphasize realism. "
    "Do not include people, faces, text, books, documents, diagrams, illustrations, animations, symbols, or abstract concepts. "
    f"The scene must visually demonstrate the practical function of the {market_name} through the objects and setup alone."
    )
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
 
    scene_prompt = response.text.strip()
    print("LLM Scene:", scene_prompt)
 
    return scene_prompt
 
def generate_market_image(market_name: str):
    print(f"Generating image for: {market_name}")
 
    base = Image.open(TEMPLATE_PATH).convert("RGB")
    base = base.resize(OUTPUT_SIZE)
    draw = ImageDraw.Draw(base)
 
    font_size = MARKET_TEXT_FONT_SIZE
    min_font_size = 20
 
    while True:
        font = load_font(font_size)
        lines = wrap_text(draw, market_name, font, MARKET_TEXT_MAX_WIDTH)
 
        if len(lines) <= 2:
            break
 
        if font_size <= min_font_size:
            lines = force_two_lines(market_name)
            font = load_font(min_font_size)
            font_size = min_font_size
            break
 
        font_size -= 1
 
    y = MARKET_TEXT_POSITION[1]
    for line in lines:
        draw.text((MARKET_TEXT_POSITION[0], y), line, fill="white", font=font)
        y += int(font_size * 1.2)
 
    # Generate AI image
    client = genai.Client(vertexai=True, api_key=GEMINI_API_KEY)
    scene_prompt = generate_scene_prompt_with_llm(market_name)
 
    # Imagen prompt
    prompt = f"Real-life photograph, {scene_prompt}"
    response = client.models.generate_images(
        model="imagen-4.0-fast-generate-001",
        prompt=prompt,
        config={
            "number_of_images": 1,
            "output_mime_type": "image/png",
            "aspect_ratio": "16:9",
        },
    )
 
    gen_img = genai_image_to_pil(response.generated_images[0].image)
 
    box_w = IMAGE_BOX[2] - IMAGE_BOX[0]  
    box_h = IMAGE_BOX[3] - IMAGE_BOX[1]  
   
    print(f"Image box dimensions: {box_w}x{box_h}")
    print(f"Image position: left={IMAGE_BOX[0]}px, top={IMAGE_BOX[1]}px")
   
    gen_img = gen_img.resize((box_w, box_h), Image.Resampling.LANCZOS)
    base.paste(gen_img, (IMAGE_BOX[0], IMAGE_BOX[1]))
 
    # Create images folder if it doesn't exist
    images_folder = "images"
    os.makedirs(images_folder, exist_ok=True)
   
    safe_name = market_name.replace(" ", "_").replace("/", "-")
    output_file = os.path.join(images_folder, f"{safe_name}.webp")
 
    final_quality, final_size = compress_webp_under_target(base, output_file, target_kb=18)
 
    print(f"Final file: {output_file}")
    print(f"Final size: {final_size:.2f} KB")
    print(f"WebP quality used: {final_quality}")
 
    parent_folder = os.path.dirname(os.getcwd())
    rel_path = os.path.relpath(output_file, start=parent_folder)
    rel_path = "\\" + rel_path.replace("/", "\\")
    print(f"Saved: {rel_path}")
    print(f"Final placement: x={IMAGE_BOX[0]}-{IMAGE_BOX[2]}, y={IMAGE_BOX[1]}-{IMAGE_BOX[3]}")
    print(f"Dimensions: {box_w}px × {box_h}px")
   
    return output_file
 
if __name__ == "__main__":
    generate_market_image("Depilatory Wax Market")