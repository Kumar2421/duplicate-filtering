import React, { useState, useEffect } from 'react';
import { Check, X, Loader2, Info } from 'lucide-react';

interface PendingImage {
  filename: string;
  prediction: string;
  confidence: number;
  timestamp: string;
}

const IDCardVerification: React.FC = () => {
  const [images, setImages] = useState<PendingImage[]>([]);
  const [loading, setLoading] = useState(true);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [processing, setProcessing] = useState(false);

  // Use a relative path or the current origin to avoid CORS loopback issues
  const API_BASE = '/api/id-workflow';

  const fetchPending = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE}/pending`);
      const data = await response.json();
      setImages(data);
    } catch (error) {
      console.error('Error fetching images:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPending();
  }, []);

  const handleVerify = async (isIdCard: boolean) => {
    if (images.length === 0) return;

    setProcessing(true);
    const current = images[currentIndex];
    const formData = new FormData();
    formData.append('filename', current.filename);
    formData.append('is_id_card', String(isIdCard));

    try {
      await fetch(`${API_BASE}/verify`, {
        method: 'POST',
        body: formData,
      });

      // Remove from local list
      const nextImages = images.filter((_, i) => i !== currentIndex);
      setImages(nextImages);
      if (currentIndex >= nextImages.length && nextImages.length > 0) {
        setCurrentIndex(nextImages.length - 1);
      }
    } catch (error) {
      console.error('Error verifying image:', error);
    } finally {
      setProcessing(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
      </div>
    );
  }

  if (images.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full p-8 text-center">
        <div className="bg-green-100 p-4 rounded-full mb-4">
          <Check className="w-12 h-12 text-green-500" />
        </div>
        <h2 className="text-2xl font-bold text-gray-800">Queue Empty</h2>
        <p className="text-gray-500 mt-2">All images have been verified.</p>
        <button
          onClick={fetchPending}
          className="mt-6 px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition"
        >
          Refresh
        </button>
      </div>
    );
  }

  const current = images[currentIndex];

  return (
    <div className="p-8 max-w-4xl mx-auto">
      <header className="mb-8 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">ID Card Verification</h1>
          <p className="text-gray-500">Review pending detections for accuracy</p>
        </div>
        <div className="bg-blue-50 text-blue-700 px-4 py-2 rounded-lg font-medium">
          {images.length} items remaining
        </div>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        {/* Image Display */}
        <div className="bg-white rounded-2xl shadow-xl overflow-hidden border-2 border-gray-100 flex items-center justify-center bg-gray-50 aspect-square">
          <img
            src={`${API_BASE}/image/${current.filename}`}
            alt="Verification subject"
            className="max-w-full max-h-full object-contain shadow-sm"
          />
        </div>

        {/* Info & Controls */}
        <div className="flex flex-col justify-between py-4">
          <div className="space-y-6">
            <div className="bg-white p-6 rounded-xl border border-gray-200">
              <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Model Prediction</h3>
              <div className="flex items-center gap-4">
                <span className={`px-4 py-2 rounded-full font-bold text-lg ${current.prediction === 'id_card' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                  }`}>
                  {current.prediction === 'id_card' ? 'ID Card' : 'No ID Card'}
                </span>
                <div className="flex flex-col">
                  <span className="text-2xl font-bold text-gray-800">{(current.confidence * 100).toFixed(1)}%</span>
                  <span className="text-xs text-gray-400">Confidence</span>
                </div>
              </div>
            </div>

            <div className="bg-amber-50 p-4 rounded-xl flex gap-3 border border-amber-100">
              <Info className="w-5 h-5 text-amber-500 flex-shrink-0" />
              <p className="text-sm text-amber-800">
                Confirm if the person in the image is wearing an ID card. Your response will move this image to the correct training folder.
              </p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4 mt-8">
            <button
              disabled={processing}
              onClick={() => handleVerify(false)}
              className="flex items-center justify-center gap-2 py-4 rounded-xl border-2 border-red-500 text-red-600 font-bold hover:bg-red-50 hover:shadow-lg transition-all disabled:opacity-50"
            >
              <X className="w-6 h-6" />
              NO (NOT WEARING)
            </button>
            <button
              disabled={processing}
              onClick={() => handleVerify(true)}
              className="flex items-center justify-center gap-2 py-4 rounded-xl bg-green-500 text-white font-bold hover:bg-green-600 hover:shadow-lg transition-all disabled:opacity-50"
            >
              <Check className="w-6 h-6" />
              YES (WEARING)
            </button>
          </div>
        </div>
      </div>

      <div className="mt-12">
        <h3 className="text-gray-500 text-sm font-medium mb-4">Queue Preview</h3>
        <div className="flex gap-4 overflow-x-auto pb-4 scrollbar-hide">
          {images.map((img, idx) => (
            <div
              key={img.filename}
              onClick={() => setCurrentIndex(idx)}
              className={`w-20 h-20 rounded-lg flex-shrink-0 cursor-pointer border-2 transition-all ${idx === currentIndex ? 'border-blue-500 scale-105 shadow-md' : 'border-transparent opacity-60 hover:opacity-100'
                }`}
            >
              <img src={`${API_BASE}/image/${img.filename}`} className="w-full h-full object-cover rounded-md" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default IDCardVerification;
