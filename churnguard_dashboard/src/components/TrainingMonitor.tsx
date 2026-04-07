import { useState, useEffect } from 'react';
import { Terminal, Activity, Server, Cpu, Database } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const mockLossData = Array.from({ length: 50 }, (_, i) => ({
  epoch: i,
  train_loss: 0.8 * Math.exp(-i / 10) + 0.1 + Math.random() * 0.05,
  val_loss: 0.8 * Math.exp(-i / 12) + 0.15 + Math.random() * 0.08,
}));

export default function TrainingMonitor() {
  const [logs, setLogs] = useState<string[]>([]);
  
  useEffect(() => {
    const initialLogs = [
      "[INFO] Initializing CustomMLP Training...",
      "[INFO] Loading feature_config.yaml...",
      "[INFO] GPU Detected: NVIDIA RTX 4090 (24GB VRAM)",
      "[INFO] MemoryManager: Initialized. Current VRAM usage: 1.2GB",
      "[INFO] Dataset: 5600 rows, 20 features.",
      "[INFO] Applying SMOTE for class imbalance...",
      "[INFO] Starting Epoch 1/50...",
    ];
    
    setLogs(initialLogs);
    
    let epoch = 1;
    const interval = setInterval(() => {
      if (epoch <= 50) {
        const trainLoss = (0.8 * Math.exp(-epoch / 10) + 0.1).toFixed(4);
        const valLoss = (0.8 * Math.exp(-epoch / 12) + 0.15).toFixed(4);
        const auc = (0.5 + 0.4 * (1 - Math.exp(-epoch / 15))).toFixed(4);
        
        setLogs(prev => [...prev, `[Epoch ${epoch}/50] Train Loss: ${trainLoss} | Val Loss: ${valLoss} | Val AUC: ${auc}`]);
        epoch++;
      } else {
        clearInterval(interval);
        setLogs(prev => [...prev, "[SUCCESS] Training Complete.", "[INFO] Registering model to MLflow...", "[INFO] Artifacts saved to models/artifacts/"]);
      }
    }, 800);
    
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="p-8 max-w-6xl mx-auto bg-gray-950 min-h-full text-gray-300">
      <div className="mb-8 flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-white flex items-center">
            <Terminal className="mr-3 text-green-400" />
            Training Monitor
          </h2>
          <p className="text-gray-400 mt-2">Local GPU Training & MLflow Logging</p>
        </div>
        <div className="flex space-x-4">
          <div className="bg-gray-900 px-4 py-2 rounded-lg border border-gray-800 flex items-center">
            <Cpu className="text-blue-400 mr-2" size={18} />
            <span className="text-sm font-mono">GPU: 68%</span>
          </div>
          <div className="bg-gray-900 px-4 py-2 rounded-lg border border-gray-800 flex items-center">
            <Database className="text-purple-400 mr-2" size={18} />
            <span className="text-sm font-mono">VRAM: 8.4GB</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-gray-900 rounded-xl border border-gray-800 p-4 flex flex-col h-[500px]">
          <div className="flex items-center justify-between mb-4 border-b border-gray-800 pb-2">
            <h3 className="text-white font-semibold flex items-center">
              <Activity className="mr-2 text-blue-400" size={18} />
              Live Logs
            </h3>
            <div className="flex space-x-2">
              <div className="w-3 h-3 rounded-full bg-red-500"></div>
              <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
              <div className="w-3 h-3 rounded-full bg-green-500"></div>
            </div>
          </div>
          <div className="flex-1 overflow-y-auto font-mono text-xs space-y-1 scrollbar-thin scrollbar-thumb-gray-700">
            {logs.map((log, i) => (
              <div key={i} className={`${log.includes('[SUCCESS]') ? 'text-green-400' : log.includes('Val AUC') ? 'text-blue-300' : 'text-gray-400'}`}>
                {log}
              </div>
            ))}
          </div>
        </div>

        <div className="flex flex-col space-y-6">
          <div className="bg-gray-900 rounded-xl border border-gray-800 p-4 h-[240px]">
            <h3 className="text-white font-semibold mb-4 text-sm">Loss Curve</h3>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={mockLossData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="epoch" stroke="#9CA3AF" fontSize={12} />
                <YAxis stroke="#9CA3AF" fontSize={12} />
                <Tooltip contentStyle={{ backgroundColor: '#111827', borderColor: '#374151' }} />
                <Line type="monotone" dataKey="train_loss" stroke="#3B82F6" strokeWidth={2} dot={false} name="Train Loss" />
                <Line type="monotone" dataKey="val_loss" stroke="#10B981" strokeWidth={2} dot={false} name="Val Loss" />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-900 rounded-xl border border-gray-800 p-4 flex-1">
            <h3 className="text-white font-semibold mb-4 text-sm">Evaluation Metrics</h3>
            <div className="space-y-4">
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span>AUC-ROC</span>
                  <span className="text-green-400">0.89</span>
                </div>
                <div className="w-full bg-gray-800 rounded-full h-2">
                  <div className="bg-green-500 h-2 rounded-full" style={{ width: '89%' }}></div>
                </div>
              </div>
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span>Recall @ Threshold</span>
                  <span className="text-blue-400">0.82</span>
                </div>
                <div className="w-full bg-gray-800 rounded-full h-2">
                  <div className="bg-blue-500 h-2 rounded-full" style={{ width: '82%' }}></div>
                </div>
              </div>
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span>PR-AUC</span>
                  <span className="text-purple-400">0.76</span>
                </div>
                <div className="w-full bg-gray-800 rounded-full h-2">
                  <div className="bg-purple-500 h-2 rounded-full" style={{ width: '76%' }}></div>
                </div>
              </div>
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span>F1 Score</span>
                  <span className="text-yellow-400">0.79</span>
                </div>
                <div className="w-full bg-gray-800 rounded-full h-2">
                  <div className="bg-yellow-500 h-2 rounded-full" style={{ width: '79%' }}></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
