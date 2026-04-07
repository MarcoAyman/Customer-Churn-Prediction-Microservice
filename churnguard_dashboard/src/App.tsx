import { useState } from 'react';
import Sidebar from './components/Sidebar';
import Flowchart from './components/Flowchart';
import FolderStructure from './components/FolderStructure';
import TrainingMonitor from './components/TrainingMonitor';
import EdaDashboard from './components/EdaDashboard';

export default function App() {
  const [activeTab, setActiveTab] = useState('flowchart');

  return (
    <div className="flex h-screen bg-gray-50 text-gray-900 font-sans">
      <Sidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      <main className="flex-1 overflow-y-auto">
        {activeTab === 'flowchart' && <Flowchart />}
        {activeTab === 'structure' && <FolderStructure />}
        {activeTab === 'eda' && <EdaDashboard />}
        {activeTab === 'monitor' && <TrainingMonitor />}
      </main>
    </div>
  );
}
