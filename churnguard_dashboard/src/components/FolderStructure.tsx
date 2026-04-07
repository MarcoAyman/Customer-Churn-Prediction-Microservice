import { useState } from 'react';
import { ChevronRight, ChevronDown, Folder, FileCode, Box, FunctionSquare } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';

const structure = [
  {
    name: 'src',
    type: 'folder',
    children: [
      {
        name: 'pipeline',
        type: 'folder',
        children: [
          {
            name: 'features.py',
            type: 'file',
            classes: [
              {
                name: 'FeatureEngineer',
                methods: ['financial_stress_signals', 'service_dependency_score', 'engagement_decay', 'contract_risk', 'category_volatility', 'fit_transform']
              }
            ]
          },
          {
            name: 'ingest.py',
            type: 'file',
            classes: [
              { name: 'DataIngestor', methods: ['load_data', 'validate_schema'] }
            ]
          },
          {
            name: 'validate.py',
            type: 'file',
            classes: [
              { name: 'DataValidator', methods: ['check_missing', 'check_outliers'] }
            ]
          },
          {
            name: 'batch_score.py',
            type: 'file',
            classes: [
              { name: 'BatchScorer', methods: ['run_batch', 'save_predictions'] }
            ]
          }
        ]
      },
      {
        name: 'ml',
        type: 'folder',
        children: [
          {
            name: 'mlp.py',
            type: 'file',
            classes: [
              {
                name: 'CustomMLP',
                methods: ['__init__', 'forward', 'backward', 'compute_loss', 'activation_fn', 'update_weights']
              }
            ]
          },
          {
            name: 'train.py',
            type: 'file',
            classes: [
              { name: 'ModelTrainer', methods: ['train_xgboost', 'train_mlp', 'log_mlflow'] }
            ]
          },
          {
            name: 'tune.py',
            type: 'file',
            classes: [
              { name: 'HyperparameterTuner', methods: ['objective', 'run_optuna'] }
            ]
          },
          {
            name: 'evaluate.py',
            type: 'file',
            classes: [
              { name: 'Evaluator', methods: ['auc_roc', 'recall_at_threshold', 'precision_at_threshold', 'f1_score', 'pr_auc', 'business_cost_score'] }
            ]
          },
          {
            name: 'memory_management.py',
            type: 'file',
            classes: [
              { name: 'MemoryManager', methods: ['monitor_gpu', 'clear_cache', 'batch_generator'] }
            ]
          }
        ]
      },
      {
        name: 'api',
        type: 'folder',
        children: [
          {
            name: 'main.py',
            type: 'file',
            classes: [
              { name: 'FastAPIApp', methods: ['create_app', 'health_check'] }
            ]
          },
          {
            name: 'routes',
            type: 'folder',
            children: [
              {
                name: 'predict.py',
                type: 'file',
                classes: [
                  { name: 'PredictRouter', methods: ['predict_single', 'predict_batch'] }
                ]
              }
            ]
          }
        ]
      },
      {
        name: 'config',
        type: 'folder',
        children: [
          { name: 'model_config.yaml', type: 'file', classes: [] },
          { name: 'feature_config.yaml', type: 'file', classes: [] }
        ]
      }
    ]
  }
];

const TreeNode = ({ node, level = 0 }: { node: any, level?: number }) => {
  const [isOpen, setIsOpen] = useState(true);
  const isFolder = node.type === 'folder';

  return (
    <div className="font-mono text-sm">
      <div 
        className={`flex items-center py-1.5 px-2 hover:bg-gray-100 rounded cursor-pointer ${level === 0 ? 'font-semibold' : ''}`}
        style={{ paddingLeft: `${level * 1.5 + 0.5}rem` }}
        onClick={() => isFolder && setIsOpen(!isOpen)}
      >
        {isFolder ? (
          <span className="mr-1 text-gray-500">
            {isOpen ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
          </span>
        ) : (
          <span className="mr-1 w-4"></span>
        )}
        
        {isFolder ? (
          <Folder size={16} className="mr-2 text-blue-500" />
        ) : (
          <FileCode size={16} className="mr-2 text-gray-500" />
        )}
        
        <span className={isFolder ? 'text-gray-800' : 'text-gray-600'}>{node.name}</span>
      </div>

      <AnimatePresence>
        {isOpen && node.children && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="overflow-hidden"
          >
            {node.children.map((child: any, i: number) => (
              <TreeNode key={i} node={child} level={level + 1} />
            ))}
          </motion.div>
        )}
      </AnimatePresence>

      {isOpen && node.classes && node.classes.length > 0 && (
        <div className="mt-1 mb-2">
          {node.classes.map((cls: any, i: number) => (
            <div key={i}>
              <div 
                className="flex items-center py-1 px-2 text-purple-600"
                style={{ paddingLeft: `${(level + 1) * 1.5 + 0.5}rem` }}
              >
                <Box size={14} className="mr-2" />
                <span className="font-semibold">class {cls.name}</span>
              </div>
              {cls.methods.map((method: string, j: number) => (
                <div 
                  key={j}
                  className="flex items-center py-0.5 px-2 text-green-600 text-xs"
                  style={{ paddingLeft: `${(level + 2) * 1.5 + 0.5}rem` }}
                >
                  <FunctionSquare size={12} className="mr-2" />
                  <span>def {method}()</span>
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default function FolderStructure() {
  return (
    <div className="p-8 max-w-5xl mx-auto">
      <div className="mb-8">
        <h2 className="text-3xl font-bold text-gray-900">System Architecture</h2>
        <p className="text-gray-500 mt-2">Directory structure, classes, and functions for the ML pipeline.</p>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        {structure.map((node, i) => (
          <TreeNode key={i} node={node} />
        ))}
      </div>
    </div>
  );
}
