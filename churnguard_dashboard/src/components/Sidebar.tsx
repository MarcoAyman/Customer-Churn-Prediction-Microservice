import { LayoutDashboard, FolderTree, Activity, TerminalSquare } from 'lucide-react';
import clsx from 'clsx';

export default function Sidebar({ activeTab, setActiveTab }: { activeTab: string, setActiveTab: (tab: string) => void }) {
  const tabs = [
    { id: 'flowchart', label: 'Pipeline Flowchart', icon: LayoutDashboard },
    { id: 'structure', label: 'System Architecture', icon: FolderTree },
    { id: 'eda', label: 'EDA Dashboard', icon: Activity },
    { id: 'monitor', label: 'Training Monitor', icon: TerminalSquare },
  ];

  return (
    <div className="w-64 bg-gray-900 text-white flex flex-col shadow-xl">
      <div className="p-6">
        <h1 className="text-2xl font-bold tracking-tight text-blue-400">ChurnGuard</h1>
        <p className="text-xs text-gray-400 mt-1">MLOps Pipeline Dashboard</p>
      </div>
      <nav className="flex-1 px-4 space-y-2">
        {tabs.map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={clsx(
                "w-full flex items-center space-x-3 px-4 py-3 rounded-lg transition-colors text-sm font-medium",
                activeTab === tab.id 
                  ? "bg-blue-600 text-white" 
                  : "text-gray-300 hover:bg-gray-800 hover:text-white"
              )}
            >
              <Icon size={18} />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </nav>
      <div className="p-4 border-t border-gray-800">
        <div className="flex items-center space-x-3">
          <div className="w-8 h-8 rounded-full bg-blue-500 flex items-center justify-center text-sm font-bold">
            ML
          </div>
          <div>
            <p className="text-sm font-medium">ML Engineer</p>
            <p className="text-xs text-green-400">● Local GPU Active</p>
          </div>
        </div>
      </div>
    </div>
  );
}
