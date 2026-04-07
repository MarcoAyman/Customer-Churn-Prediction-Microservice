import { motion } from 'motion/react';
import { Database, Cpu, Activity, Server, ArrowRight } from 'lucide-react';

export default function Flowchart() {
  const steps = [
    {
      id: 1,
      title: "Data Ingestion & Validation",
      icon: Database,
      desc: "Load raw data, validate schema, handle missing values.",
      color: "bg-indigo-500"
    },
    {
      id: 2,
      title: "Feature Engineering",
      icon: Activity,
      desc: "Extract domain signals: Financial Stress, Service Dependency, Engagement Decay.",
      color: "bg-blue-500"
    },
    {
      id: 3,
      title: "Model Training & Tuning",
      icon: Cpu,
      desc: "Train Custom MLP & XGBoost. Cross-validation, Optuna tuning, MLflow tracking.",
      color: "bg-purple-500"
    },
    {
      id: 4,
      title: "Evaluation & Registry",
      icon: Server,
      desc: "Evaluate metrics (AUC-ROC, PR-AUC, F1). Register champion model.",
      color: "bg-emerald-500"
    }
  ];

  return (
    <div className="p-8 max-w-5xl mx-auto">
      <div className="mb-8">
        <h2 className="text-3xl font-bold text-gray-900">ML Pipeline Flowchart</h2>
        <p className="text-gray-500 mt-2">End-to-end professional machine learning pipeline architecture.</p>
      </div>

      <div className="relative">
        <div className="absolute top-1/2 left-0 w-full h-1 bg-gray-200 -translate-y-1/2 z-0 hidden md:block"></div>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 relative z-10">
          {steps.map((step, index) => (
            <motion.div 
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.2 }}
              key={step.id} 
              className="bg-white rounded-xl shadow-lg p-6 border border-gray-100 flex flex-col items-center text-center relative"
            >
              <div className={`w-16 h-16 rounded-full ${step.color} text-white flex items-center justify-center mb-4 shadow-md`}>
                <step.icon size={28} />
              </div>
              <h3 className="text-lg font-semibold text-gray-900 mb-2">{step.title}</h3>
              <p className="text-sm text-gray-500">{step.desc}</p>
              
              {index < steps.length - 1 && (
                <div className="absolute -right-4 top-1/2 -translate-y-1/2 text-gray-400 hidden md:block bg-white rounded-full p-1 z-20">
                  <ArrowRight size={20} />
                </div>
              )}
            </motion.div>
          ))}
        </div>
      </div>

      <div className="mt-12 grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <h4 className="font-semibold text-gray-900 mb-4">Pipeline Artifacts</h4>
          <ul className="space-y-3 text-sm text-gray-600">
            <li className="flex items-center"><div className="w-2 h-2 bg-blue-500 rounded-full mr-2"></div> <code>preprocessor.pkl</code> - Fitted sklearn Pipeline</li>
            <li className="flex items-center"><div className="w-2 h-2 bg-blue-500 rounded-full mr-2"></div> <code>feature_names.json</code> - Ordered feature list</li>
            <li className="flex items-center"><div className="w-2 h-2 bg-blue-500 rounded-full mr-2"></div> <code>category_churn_rates.json</code> - Category mapping</li>
            <li className="flex items-center"><div className="w-2 h-2 bg-blue-500 rounded-full mr-2"></div> <code>reference_distribution.pkl</code> - For drift checks</li>
          </ul>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <h4 className="font-semibold text-gray-900 mb-4">Class Imbalance Strategy</h4>
          <ol className="space-y-3 text-sm text-gray-600 list-decimal list-inside">
            <li><code>scale_pos_weight</code> in XGBoost</li>
            <li><code>class_weight='balanced'</code> in Logistic Regression</li>
            <li>Threshold tuning post-training (PR curve)</li>
            <li>SMOTE as fallback (training data only)</li>
          </ol>
        </div>
      </div>
    </div>
  );
}
