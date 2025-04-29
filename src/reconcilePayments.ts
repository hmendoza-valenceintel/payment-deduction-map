import db, {
  RemittanceInvoices,
  BillingLedger,
  VendorCase,
  Vendor,
} from '@valencemi/amazon_vendor_central_db_model';
import { Sequelize, Transaction, Op } from 'sequelize';
import 'dotenv/config';

// Global counter for mapped payments
let mappedPaymentsCount = 0;

// Load environment variables (use dotenv if needed)
const { DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT } = process.env;

function connectToDatabase() {
  db.connect({
    DB_Name: DB_NAME as string,
    DB_Username: DB_USER as string,
    DB_Password: DB_PASSWORD as string,
    DB_Host: DB_HOST as string,
    DB_Port: DB_PORT ? parseInt(DB_PORT) : 3306,
  });
  // Disable Sequelize logging after connection
  db.sequelize.options.logging = false;
  return db.sequelize;
}

async function fetchUnmappedPayments() {
  return RemittanceInvoices.findAll({
    where: {
      InvoiceAmount: { [Op.gt]: 0 }, // positive amount
      PaymentRemittanceId: { [Op.is]: null as any }, // not mapped
    },
    raw: true,
  });
}

async function fetchUnmappedDeductions() {
  return RemittanceInvoices.findAll({
    where: {
      InvoiceAmount: { [Op.lt]: 0 }, // negative amount
      PaymentRemittanceId: { [Op.is]: null as any }, // not mapped
    },
    raw: true,
  });
}

async function fetchVendorCases(deductionsVendorCaseIds: number[]) {
  return VendorCase.findAll({
    where: { Id: { [Op.in]: deductionsVendorCaseIds } },
    raw: true,
  });
}

async function fetchVendors(vendorIds: number[]) {
  return Vendor.findAll({
    where: { Id: { [Op.in]: vendorIds } },
    raw: true,
  });
}

function buildDeductionMap(deductions: any[]) {
  // Build a map of deductions by <key, deductions>
  const deductionMap = new Map();
  for (const deduction of deductions) {
    const key = `${deduction.VendorId}|${deduction.RootInvoiceNumber}`;
    if (!deductionMap.has(key)) deductionMap.set(key, []);
    (deductionMap.get(key) as any[]).push(deduction);
  }
  return deductionMap;
}

async function processPayments(
  payments: any[], // List of payments
  deductionMap: Map<string, any[]>, // Map of deductions by <key, deductions>
  vendorCaseMap: Map<number, any>, // Map of vendor cases by <id, vendorCase>
  vendorMap: Map<number, any>, // Map of vendors by <id, vendor>
  sequelize: Sequelize
) {
  for (const payment of payments) {
    // Determine payment claim type
    const paymentIsShortageClaim = payment.SubInvoiceNumber?.includes('SC'); // SC = Shortage Claim
    const paymentIsPriceClaim = payment.SubInvoiceNumber?.includes('PC'); // PC = Price Claim
    if (!paymentIsShortageClaim && !paymentIsPriceClaim) continue; // skip if not a SC or PC claim

    // Find all deductions for this payment's key
    const key = `${payment.VendorId}|${payment.RootInvoiceNumber}`;
    const deductionsForKey = (deductionMap.get(key) || []) as any[];

    for (const deduction of deductionsForKey) {
      if (deduction.InvoiceDate > payment.InvoiceDate) continue; // Skip if deduction date is after payment date
      const mapped = await mapPaymentToDeduction(
        payment,
        deduction,
        vendorCaseMap,
        vendorMap,
        sequelize
      );
      if (mapped) break; // Only map one deduction per payment
    }
  }
}

async function mapPaymentToDeduction(
  payment: any,
  deduction: any,
  vendorCaseMap: Map<number, any>,
  vendorMap: Map<number, any>,
  sequelize: Sequelize
) {
  // Identify deduction claim type
  const deductionIsShortageClaim = deduction.SubInvoiceNumber?.includes('SC'); // SC = Shortage Claim
  const deductionIsPriceClaim = deduction.SubInvoiceNumber?.includes('PC'); // PC = Price Claim
  // Only allow SC->SC and PC->PC matches
  const paymentIsShortageClaim = payment.SubInvoiceNumber?.includes('SC');
  const paymentIsPriceClaim = payment.SubInvoiceNumber?.includes('PC');
  if (!deductionIsShortageClaim && !deductionIsPriceClaim) return false;
  if (
    (paymentIsShortageClaim && !deductionIsShortageClaim) ||
    (paymentIsPriceClaim && !deductionIsPriceClaim)
  )
    return false;

  // Amount match
  // Only match if payment amount exactly equals the absolute value of the deduction amount
  if (
    parseFloat(String(payment.InvoiceAmount ?? '0')) !==
    Math.abs(parseFloat(String(deduction.InvoiceAmount ?? '0')))
  )
    return false;

  // Check for valid Vendor Case
  if (!deduction.ReversalForVendorCaseId) return false;
  const vendorCase = vendorCaseMap.get(deduction.ReversalForVendorCaseId);
  if (!vendorCase || vendorCase.IsValidCase == null) return false;

  // Get OrganizationId from Vendor
  const vendor = vendorMap.get(vendorCase.VendorId);
  const organizationId = vendor?.OrganizationId;
  if (!organizationId) return false;

  // Update in a transaction
  try {
    await sequelize.transaction(async (t: Transaction) => {
      // Add spacing and separator for readability
      console.log(
        '\n-----------------Initializing Transaction-----------------'
      );
      console.log(
        `Processing: ${payment.InvoiceNumber} (${payment.InvoiceAmount}) -> ${deduction.InvoiceNumber} (${deduction.InvoiceAmount})`
      );
      // Update deduction
      await RemittanceInvoices.update(
        { PaymentRemittanceId: payment.Id },
        { where: { Id: deduction.Id }, transaction: t }
      );

      // Check if BillingLedger entry already exists
      const existingBillingLedger = await BillingLedger.findOne({
        where: {
          VendorId: payment.VendorId,
          OrganizationId: organizationId,
          BillingKey: String(payment.PaymentNumber ?? ''),
          BillingKey2: String(payment.InvoiceNumber ?? ''),
          KeySource: 'RemittanceInvoice',
        },
        transaction: t,
      });

      // Only create new entry if one doesn't exist
      if (!existingBillingLedger) {
        const newLedger = await BillingLedger.create(
          {
            VendorId: payment.VendorId,
            OrganizationId: organizationId,
            BillingKey: String(payment.PaymentNumber ?? ''),
            BillingKey2: String(payment.InvoiceNumber ?? ''),
            KeySource: 'RemittanceInvoice',
            Amount: String(payment.InvoiceAmount ?? ''),
            CurrencyCode: String(payment.InvoiceCurrency ?? ''),
            Description: `Billing for ${payment.InvoiceNumber}`,
            CaseId: deduction.ReversalForVendorCaseId?.toString(),
            DateCreated: new Date(),
            CreatedBy: 'auto-mapper-script',
          },
          { transaction: t }
        );
        console.log(
          `Created BillingLedger entry with id: ${newLedger.Id} for payment ${payment.InvoiceNumber}`
        );
      } else {
        console.log(
          `Skipped creating billing ledger for payment ${payment.InvoiceNumber} because it already exists with id: ${existingBillingLedger.Id}`
        );
      }
      // Add a separator after each mapping/skip
      console.log('--------------------------------------------------\n');
    });
    console.log(`Mapped payment ${payment.Id} to deduction ${deduction.Id}`);
    mappedPaymentsCount++; // Increment counter on successful mapping
    return true;
  } catch (error) {
    console.error(
      `Error mapping payment ${payment.Id} to deduction ${deduction.Id}:`,
      error
    );
    return false;
  }
}

// Helper func to build a map of vendor cases from deductions
async function buildVendorCaseMap(
  deductions: any[]
): Promise<Map<number, any>> {
  const vendorCaseIds = Array.from(
    new Set(
      deductions
        .map((d) => d.ReversalForVendorCaseId)
        .filter((id): id is number => typeof id === 'number')
    )
  ); // Using a Set constructor to avoid potential duplicates

  // Fetch Vendor Cases
  const vendorCases = await fetchVendorCases(vendorCaseIds);

  // Build a map of vendor cases by <id, vendorCase> and return it
  return new Map(
    vendorCases
      .filter((vc) => typeof vc.Id === 'number')
      .map((vc) => [vc.Id as number, vc])
  );
}

// Helper to build a map of vendors from vendorCases
async function buildVendorMap(vendorCases: any[]): Promise<Map<number, any>> {
  // Build a list of vendorIds from vendorCases
  const vendorIds = Array.from(
    new Set(
      vendorCases
        .map((vc) => vc.VendorId)
        .filter((id): id is number => typeof id === 'number')
    )
  );

  // Fetch Vendors
  const vendors = await fetchVendors(vendorIds);

  // Build a map of vendors by <id, vendor> and return it
  return new Map(
    vendors
      .filter((v) => typeof v.Id === 'number')
      .map((v) => [v.Id as number, v])
  );
}

export async function main() {
  const startTime = Date.now();
  const sequelize = connectToDatabase();
  try {
    // Reset counter at start
    mappedPaymentsCount = 0;

    // Step 1: Fetch unmapped payments and deductions
    const payments = await fetchUnmappedPayments();
    console.log(`Found ${payments.length} unmapped payments.`);
    const deductions = await fetchUnmappedDeductions();
    console.log(`Found ${deductions.length} unmapped deductions.`);

    // Step 2: Prepare batch data
    const vendorCaseMap = await buildVendorCaseMap(deductions);
    const vendorCasesList = Array.from(vendorCaseMap.values());
    const vendorMap = await buildVendorMap(vendorCasesList);
    const deductionMap = buildDeductionMap(deductions);

    // Step 3: Process payments mapping
    await processPayments(
      payments,
      deductionMap,
      vendorCaseMap,
      vendorMap,
      sequelize
    );
    console.log('Auto-mapping complete.');
    console.log(
      `Successfully mapped ${mappedPaymentsCount} payments out of ${payments.length} total payments.`
    );
    console.log(`Completed in ${(Date.now() - startTime) / 1000} seconds`);
  } catch (err) {
    console.error('Error during auto-mapping:', err);
    process.exit(1);
  } finally {
    await db.sequelize.close();
  }
}
