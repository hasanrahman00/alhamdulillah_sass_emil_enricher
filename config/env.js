// Configuration and environment variables
export const config = {
  // Base URL for the MailTester Ninja API
  mailTesterBaseUrl: process.env.MAILTESTER_BASE_URL || 'https://happy.mailtester.ninja/ninja',
  // URL for retrieving a MailTester subscription key (governs pacing)
  keyProviderUrl: process.env.KEY_PROVIDER_URL || 'https://api.daddy-leads.com/mailtester/key/available',
  // Number of contacts processed per batch wave during combo processing
  comboBatchSize: Number(process.env.COMBO_BATCH_SIZE) || 25,
  // Port for the HTTP server
  port: Number(process.env.PORT) || 3000,
};