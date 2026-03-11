#!/bin/bash
# Generate an unsigned Apple Wallet .pkpass file
# A .pkpass is a ZIP file containing pass.json, manifest.json, and images

set -e
cd "$(dirname "$0")"

PASS_DIR="wallet-pass"
rm -rf "$PASS_DIR/pass.pkpass"

# Generate icon images (simple colored squares with initials)
# Using sips/python to create minimal PNG images
python3 << 'PYEOF'
import struct
import zlib
import os

def create_png(width, height, r, g, b, text_color=(255,255,255)):
    """Create a minimal PNG with solid background and 'RR' text-like pattern."""

    def make_chunk(chunk_type, data):
        chunk = chunk_type + data
        return struct.pack('>I', len(data)) + chunk + struct.pack('>I', zlib.crc32(chunk) & 0xffffffff)

    # Simple solid color image
    raw = b''
    for y in range(height):
        raw += b'\x00'  # filter byte
        for x in range(width):
            # Create a subtle gradient
            fr = min(255, r + (y * 20 // height))
            fg = min(255, g + (y * 10 // height))
            fb = min(255, b + (y * 10 // height))
            raw += struct.pack('BBB', fr, fg, fb)

    compressed = zlib.compress(raw)

    png = b'\x89PNG\r\n\x1a\n'
    png += make_chunk(b'IHDR', struct.pack('>IIBBBBB', width, height, 8, 2, 0, 0, 0))
    png += make_chunk(b'IDAT', compressed)
    png += make_chunk(b'IEND', b'')
    return png

os.makedirs('wallet-pass', exist_ok=True)

# Databricks orange: #FF3621 = (255, 54, 33)
icon = create_png(29, 29, 255, 54, 33)
icon2x = create_png(58, 58, 255, 54, 33)
logo = create_png(160, 50, 255, 54, 33)
logo2x = create_png(320, 100, 255, 54, 33)

with open('wallet-pass/icon.png', 'wb') as f: f.write(icon)
with open('wallet-pass/icon@2x.png', 'wb') as f: f.write(icon2x)
with open('wallet-pass/logo.png', 'wb') as f: f.write(logo)
with open('wallet-pass/logo@2x.png', 'wb') as f: f.write(logo2x)

print("Generated wallet pass images")
PYEOF

# Create pass.json
cat > "$PASS_DIR/pass.json" << 'EOF'
{
  "formatVersion": 1,
  "passTypeIdentifier": "pass.com.databricks.businesscard",
  "serialNumber": "rajesh-ramdas-001",
  "teamIdentifier": "DATABRICKS",
  "organizationName": "Databricks India",
  "description": "Rajesh Ramdas - Business Card",
  "logoText": "Databricks",
  "foregroundColor": "rgb(255, 255, 255)",
  "backgroundColor": "rgb(255, 54, 33)",
  "labelColor": "rgb(255, 220, 210)",
  "generic": {
    "primaryFields": [
      {
        "key": "name",
        "label": "NAME",
        "value": "Rajesh Ramdas"
      }
    ],
    "secondaryFields": [
      {
        "key": "title",
        "label": "TITLE",
        "value": "Solutions Architect"
      },
      {
        "key": "company",
        "label": "COMPANY",
        "value": "Databricks India"
      }
    ],
    "auxiliaryFields": [
      {
        "key": "phone",
        "label": "PHONE",
        "value": "+91 958 226 4238"
      },
      {
        "key": "email",
        "label": "EMAIL",
        "value": "rajesh.ramdas@databricks.com"
      }
    ],
    "backFields": [
      {
        "key": "contact-info",
        "label": "Contact Information",
        "value": "Rajesh Ramdas\nSolutions Architect\nDatabricks India\n\nPhone: +91 958 226 4238\nEmail: rajesh.ramdas@databricks.com"
      }
    ]
  },
  "barcode": {
    "message": "BEGIN:VCARD\nVERSION:3.0\nN:Ramdas;Rajesh;;;\nFN:Rajesh Ramdas\nORG:Databricks India\nTITLE:Solutions Architect\nTEL;TYPE=CELL:+919582264238\nEMAIL;TYPE=WORK:rajesh.ramdas@databricks.com\nEND:VCARD",
    "format": "PKBarcodeFormatQR",
    "messageEncoding": "iso-8859-1"
  },
  "barcodes": [
    {
      "message": "BEGIN:VCARD\nVERSION:3.0\nN:Ramdas;Rajesh;;;\nFN:Rajesh Ramdas\nORG:Databricks India\nTITLE:Solutions Architect\nTEL;TYPE=CELL:+919582264238\nEMAIL;TYPE=WORK:rajesh.ramdas@databricks.com\nEND:VCARD",
      "format": "PKBarcodeFormatQR",
      "messageEncoding": "iso-8859-1"
    }
  ]
}
EOF

# Create manifest.json with SHA1 hashes
python3 << 'PYEOF'
import hashlib
import json
import os

manifest = {}
pass_dir = 'wallet-pass'

for filename in ['pass.json', 'icon.png', 'icon@2x.png', 'logo.png', 'logo@2x.png']:
    filepath = os.path.join(pass_dir, filename)
    with open(filepath, 'rb') as f:
        sha1 = hashlib.sha1(f.read()).hexdigest()
    manifest[filename] = sha1

with open(os.path.join(pass_dir, 'manifest.json'), 'w') as f:
    json.dump(manifest, f, indent=2)

print("Generated manifest.json")
PYEOF

# Package as .pkpass (ZIP file)
cd "$PASS_DIR"
zip -q "../Rajesh_Ramdas.pkpass" pass.json manifest.json icon.png icon@2x.png logo.png logo@2x.png
cd ..

echo ""
echo "✓ Generated: Rajesh_Ramdas.pkpass"
echo "  Location: $(pwd)/Rajesh_Ramdas.pkpass"
echo ""
echo "NOTE: This is an unsigned pass. Apple Wallet requires signed passes."
echo "To use it, you have two options:"
echo "  1. Sign it with an Apple Developer certificate (requires paid Apple Developer account)"
echo "  2. Use a service like https://passkit.com or https://walletpasses.io to create a signed version"
echo "     using the pass.json configuration in the wallet-pass/ directory"
