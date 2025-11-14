export const metadata = {
  title: "Dashboard",
  description: "Financial Dashboard",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body className="min-h-screen flex">
        <aside className="w-60 bg-white shadow-md p-4 space-y-4">
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <nav className="flex flex-col space-y-2 text-gray-700">
            <a href="/dashboard">Overview</a>
            <a href="/dashboard/vendors">Vendors</a>
            <a href="/dashboard/categories">Categories</a>
            <a href="/dashboard/monthly">Monthly</a>
            <a href="/dashboard/anomalies">Anomalies</a>
            <a href="/dashboard/insights">Insights</a>
          </nav>
        </aside>
        <main className="flex-1 p-8">{children}</main>
      </body>
    </html>
  );
}
