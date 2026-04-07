import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

const categoryData = [
  { name: 'Grocery', churn: 5, retained: 95 },
  { name: 'Mobile', churn: 15, retained: 85 },
  { name: 'Fashion', churn: 25, retained: 75 },
  { name: 'Electronics', churn: 22, retained: 78 },
  { name: 'Others', churn: 12, retained: 88 },
];

const tenureData = [
  { name: '0-6 mo (New)', value: 45 },
  { name: '6-24 mo (Growing)', value: 35 },
  { name: '24+ mo (Loyal)', value: 20 },
];

const COLORS = ['#3B82F6', '#10B981', '#8B5CF6'];

export default function EdaDashboard() {
  return (
    <div className="p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <h2 className="text-3xl font-bold text-gray-900">Feature Engineering & EDA</h2>
        <p className="text-gray-500 mt-2">Analysis of categorical values, engagement decay, and financial stress signals.</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Category Volatility Signal</h3>
          <p className="text-sm text-gray-500 mb-6">Necessity buyers (Grocery) churn less. Discretionary buyers (Fashion) respond to better deals.</p>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={categoryData} layout="vertical" margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                <XAxis type="number" />
                <YAxis dataKey="name" type="category" width={80} fontSize={12} />
                <Tooltip />
                <Bar dataKey="churn" stackId="a" fill="#EF4444" name="Churn %" />
                <Bar dataKey="retained" stackId="a" fill="#10B981" name="Retained %" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Contract Risk Profile (Tenure)</h3>
          <p className="text-sm text-gray-500 mb-6">Churn hazard is highest for new customers. Encoding as ordinal helps the model learn the lifecycle curve.</p>
          <div className="h-64 flex items-center justify-center">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={tenureData}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={80}
                  paddingAngle={5}
                  dataKey="value"
                >
                  {tenureData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
            <div className="ml-4">
              {tenureData.map((entry, index) => (
                <div key={index} className="flex items-center mb-2 text-sm">
                  <div className="w-3 h-3 rounded-full mr-2" style={{ backgroundColor: COLORS[index] }}></div>
                  <span className="text-gray-700">{entry.name} ({entry.value}%)</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Engineered Feature Groups</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="p-4 bg-red-50 rounded-lg border border-red-100">
            <h4 className="font-semibold text-red-800 mb-2">Financial Stress</h4>
            <ul className="text-sm text-red-700 space-y-1">
              <li>• discount_dependency_ratio</li>
              <li>• cashback_per_order</li>
              <li>• order_value_declining_flag</li>
            </ul>
          </div>
          <div className="p-4 bg-blue-50 rounded-lg border border-blue-100">
            <h4 className="font-semibold text-blue-800 mb-2">Service Dependency</h4>
            <ul className="text-sm text-blue-700 space-y-1">
              <li>• platform_embeddedness_score</li>
              <li>• is_cod_user</li>
              <li>• address_per_tenure_ratio</li>
            </ul>
          </div>
          <div className="p-4 bg-purple-50 rounded-lg border border-purple-100">
            <h4 className="font-semibold text-purple-800 mb-2">Engagement Decay</h4>
            <ul className="text-sm text-purple-700 space-y-1">
              <li>• recency_risk_score</li>
              <li>• order_frequency_normalized</li>
              <li>• app_engagement_tier</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
