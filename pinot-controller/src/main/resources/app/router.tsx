import HomePage from './pages/HomePage';
import TenantsPage from './pages/Tenants';
import TenantPageDetails from './pages/TenantDetails';
import QueryPage from './pages/Query';


export default [
  { path: "/", Component: HomePage },
  { path: "/tenants/:tenantName", Component: TenantsPage },
  { path: "/tenants/:tenantName/table/:tableName", Component: TenantPageDetails },
  { path: "/query", Component: QueryPage }
];