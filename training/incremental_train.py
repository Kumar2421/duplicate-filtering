import os
import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import models, transforms, datasets
from torch.utils.data import DataLoader
import shutil

def incremental_fine_tune(model_path, feedback_dir, base_dataset_dir, output_model_path, epochs=3, lr=0.0001):
    """
    Fine-tunes the existing model using new data collected from human feedback.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # 1. Load the existing model
    model = models.efficientnet_b0()
    num_ftrs = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_ftrs, 2)
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.to(device)
    
    # 2. Prepare Data
    # In a real scenario, we combine the original dataset with the new feedback
    # For this script, we'll assume feedback_dir has 'positive' and 'negative' subfolders
    
    data_transforms = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    
    # Check if we have enough feedback data to even bother training
    pos_count = len(os.listdir(os.path.join(feedback_dir, "positive")))
    neg_count = len(os.listdir(os.path.join(feedback_dir, "negative")))
    
    print(f"New feedback data: {pos_count} positive, {neg_count} negative")
    
    if pos_count + neg_count < 10:
        print("Not enough new data for fine-tuning yet (minimum 10 samples).")
        return

    # Load feedback dataset
    feedback_dataset = datasets.ImageFolder(feedback_dir, data_transforms)
    dataloader = DataLoader(feedback_dataset, batch_size=min(8, pos_count + neg_count), shuffle=True)
    
    # 3. Training Loop (Low learning rate for fine-tuning)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    
    model.train()
    for epoch in range(epochs):
        running_loss = 0.0
        for inputs, labels in dataloader:
            inputs = inputs.to(device)
            labels = labels.to(device)
            
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            running_loss += loss.item()
            
        print(f"Epoch {epoch+1}/{epochs}, Loss: {running_loss/len(dataloader):.4f}")
    
    # 4. Save the updated model
    torch.save(model.state_dict(), output_model_path)
    print(f"Incremental training complete. Model saved to {output_model_path}")
    
    # Optional: Archive processed feedback so we don't train on it again next time
    # archive_dir = os.path.join(feedback_dir, "archived_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    # ... logic to move files ...

if __name__ == "__main__":
    MODEL_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/id_card_classifier.pth"
    FEEDBACK_DIR = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/human_feedback"
    DATA_DIR = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/classification_dataset"
    NEW_MODEL_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/id_card_classifier_v2.pth"
    
    incremental_fine_tune(MODEL_PATH, FEEDBACK_DIR, DATA_DIR, NEW_MODEL_PATH)
