import { main as reconcilePayments } from './reconcilePayments';

// Load environment variables
import 'dotenv/config';

// Run the reconciliation process
reconcilePayments()
  .then(() => {
    console.log('Reconciliation process completed successfully');
    process.exit(0);
  })
  .catch((error: Error) => {
    console.error('Error during reconciliation:', error);
    process.exit(1);
  });
