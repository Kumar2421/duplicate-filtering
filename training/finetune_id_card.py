import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, models, transforms
from torch.utils.data import DataLoader
import os
from tqdm import tqdm
from pathlib import Path

def finetune_model(data_dir, model_path, output_path, num_epochs=10, batch_size=16, lr=0.0001):
    # Minimal augmentation for fine-tuning
    data_transforms = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.RandomHorizontalFlip(), # Minimal flip
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

    # Load the verified balanced dataset
    dataset = datasets.ImageFolder(data_dir, transform=data_transforms)
    
    # Split into train and small val set (90/10)
    train_size = int(0.9 * len(dataset))
    val_size = len(dataset) - train_size
    train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, num_workers=4)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, num_workers=4)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Fine-tuning on device: {device}")
    print(f"Dataset size: {len(dataset)} (Train: {train_size}, Val: {val_size})")

    # Load existing model architecture
    model = models.efficientnet_b0()
    num_ftrs = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_ftrs, 2)

    # Load weights
    if os.path.exists(model_path):
        model.load_state_dict(torch.load(model_path, map_location=device))
        print(f"Loaded existing model from {model_path}")
    else:
        print("Warning: Base model not found, using ImageNet weights.")
        model = models.efficientnet_b0(weights='IMAGENET1K_V1')
        model.classifier[1] = nn.Linear(num_ftrs, 2)

    model = model.to(device)

    criterion = nn.CrossEntropyLoss()
    # Lower learning rate for fine-tuning
    optimizer = optim.Adam(model.parameters(), lr=lr)

    best_acc = 0.0

    for epoch in range(num_epochs):
        print(f'Epoch {epoch}/{num_epochs - 1}')
        print('-' * 10)

        for phase in ['train', 'val']:
            if phase == 'train':
                model.train()
                dataloader = train_loader
            else:
                model.eval()
                dataloader = val_loader

            running_loss = 0.0
            running_corrects = 0

            for inputs, labels in tqdm(dataloader):
                inputs = inputs.to(device)
                labels = labels.to(device)

                optimizer.zero_grad()

                with torch.set_grad_enabled(phase == 'train'):
                    outputs = model(inputs)
                    _, preds = torch.max(outputs, 1)
                    loss = criterion(outputs, labels)

                    if phase == 'train':
                        loss.backward()
                        optimizer.step()

                running_loss += loss.item() * inputs.size(0)
                running_corrects += torch.sum(preds == labels.data)

            epoch_loss = running_loss / (train_size if phase == 'train' else val_size)
            epoch_acc = running_corrects.double() / (train_size if phase == 'train' else val_size)

            print(f'{phase} Loss: {epoch_loss:.4f} Acc: {epoch_acc:.4f}')

            if phase == 'val' and epoch_acc >= best_acc:
                best_acc = epoch_acc
                torch.save(model.state_dict(), output_path)
                print(f"Saved fine-tuned model to {output_path}")

    print(f'Fine-tuning complete. Best Val Acc: {best_acc:4f}')

if __name__ == "__main__":
    # Use the full classification dataset directory which now includes:
    # 1. Original Dataset
    # 2. Dataset 2
    # 3. New Roboflow Dataset (merged via convert_to_classification.py)
    DATASET_DIR = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/classification_dataset/train"
    MODEL_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/id_card_classifier_v1.pth"
    OUTPUT_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/id_card_classifier_v2.pth"
    
    # Run fine-tuning with the expanded dataset
    finetune_model(DATASET_DIR, MODEL_PATH, OUTPUT_PATH, num_epochs=15, batch_size=32)
