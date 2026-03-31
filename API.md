# Duplicate Filtering API Documentation

This document describes the face recognition and comparison APIs available in the system.

## 1. Check Enrollment
Checks if a person in the provided image is already enrolled in the system for a specific branch and date.

**Endpoint:** `POST /api/check-enrollment`

### Request Body
| Field | Type | Description |
| :--- | :--- | :--- |
| `image` | string | Full local path or remote URL of the image. |
| `branch` | string | Branch ID to search within. |
| `date` | string | Date (YYYY-MM-DD) to search within. |
| `run_liveness` | boolean | (Optional) Whether to run DeepFace anti-spoofing. |
| `liveness_backend` | string | (Optional) Backend for liveness (`opencv`, `retinaface`, etc). |
| `return_crops` | boolean | (Optional) Return base64 face crops in response. |
| `crop_padding` | integer | (Optional) Padding in pixels for returned crops. |

### Example Request
```bash
curl -X POST http://127.0.0.1:8009/api/check-enrollment \
  -H "Content-Type: application/json" \
  -d '{
    "image": "/path/to/image.jpg",
    "branch": "TMJ-CBE",
    "date": "2026-03-25",
    "run_liveness": true
  }'
```

---

## 2. Face Comparison
Compares faces in two different images and returns a similarity score.

**Endpoint:** `POST /api/compare-faces`

### Request Body
| Field | Type | Description |
| :--- | :--- | :--- |
| `image1` | string | Path or URL for the first image. |
| `image2` | string | Path or URL for the second image. |
| `threshold` | float | (Optional) Custom similarity threshold (0.0 to 1.0). Defaults to system config (0.75). |
| `return_crops` | boolean | (Optional) Return base64 face crops for both images. |
| `crop_padding` | integer | (Optional) Padding for returned crops. |

### Example Request
```bash
curl -X POST http://127.0.0.1:8009/api/compare-faces \
  -H "Content-Type: application/json" \
  -d '{
    "image1": "/data/test1.jpg",
    "image2": "/data/test2.jpg",
    "threshold": 0.80
  }'
```

### Response Schema
```json
{
  "ok": true,
  "similarity": 0.92,
  "is_match": true,
  "threshold": 0.80,
  "image1": {
    "passed": true,
    "quality": 0.85,
    "persons_count": 1,
    "bboxes": [{"x1": 10, "y1": 10, "x2": 100, "y2": 100, "score": 0.99}]
  },
  "image2": { ... },
  "model_meta": { "name": "buffalo_l", "threshold": 0.80 }
}
```
