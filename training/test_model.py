import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image
import os

def test_single_image(model_path, image_path):
    # Set device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Load Model
    model = models.efficientnet_b0()
    num_ftrs = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_ftrs, 2)
    
    if not os.path.exists(model_path):
        print(f"Error: Model file not found at {model_path}")
        return

    model.load_state_dict(torch.load(model_path, map_location=device))
    model.to(device)
    model.eval()

    # Image Transform (same as used in training)
    transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

    # Load and process image
    if not os.path.exists(image_path):
        print(f"Error: Image file not found at {image_path}")
        return

    image = Image.open(image_path).convert('RGB')
    input_tensor = transform(image).unsqueeze(0).to(device)

    # Inference
    with torch.no_grad():
        outputs = model(input_tensor)
        probabilities = torch.nn.functional.softmax(outputs, dim=1)
        prob_id = probabilities[0][0].item()
        prob_no_id = probabilities[0][1].item()
    
    classes = ['id_card', 'no_id_card']
    prediction = classes[0] if prob_id > prob_no_id else classes[1]
    confidence = max(prob_id, prob_no_id)

    print("\n--- Test Results ---")
    print(f"Image: {os.path.basename(image_path)}")
    print(f"Prediction: {prediction}")
    print(f"Confidence: {confidence:.4f}")
    print(f"Probabilities: ID Card: {prob_id:.4f}, No ID Card: {prob_no_id:.4f}")
    print("--------------------")

    # Save the result image with text overlay
    import cv2
    import numpy as np
    
    # Load image with OpenCV for drawing
    img_cv = cv2.imread(image_path)
    if img_cv is not None:
        text = f"{prediction} ({confidence:.2f})"
        color = (0, 255, 0) if prediction == 'id_card' else (0, 0, 255)
        
        # Add background rectangle for text readability
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 1.0
        thickness = 2
        (text_w, text_h), baseline = cv2.getTextSize(text, font, font_scale, thickness)
        cv2.rectangle(img_cv, (10, 10), (10 + text_w, 20 + text_h), (0, 0, 0), -1)
        cv2.putText(img_cv, text, (10, 15 + text_h), font, font_scale, color, thickness)
        
        output_path = os.path.join(os.path.dirname(image_path), f"result_{os.path.basename(image_path)}")
        cv2.imwrite(output_path, img_cv)
        print(f"Annotated image saved to: {output_path}")

if __name__ == "__main__":
    MODEL_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/id_card_classifier_v2.pth"
    IMAGE_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/assets/73ca1984-5b65-4a88-ade7-e7bbce0b6136-4192026-115228PM.jpg"
    
    test_single_image(MODEL_PATH, IMAGE_PATH)
