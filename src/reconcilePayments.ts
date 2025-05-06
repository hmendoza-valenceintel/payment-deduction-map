import db, {
  RemittanceInvoices,
  BillingLedger,
  VendorCase,
  Vendor,
  AuditManual,
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
      InvoiceCurrency: 'USD',
    },
  });
}

async function fetchUnmappedDeductions() {
  return RemittanceInvoices.findAll({
    where: {
      InvoiceAmount: { [Op.lt]: 0 }, // negative amount
      PaymentRemittanceId: { [Op.is]: null as any }, // not mapped
      InvoiceCurrency: 'USD',
    },
    include: [
      {
        model: AuditManual,
        required: true,
        include: [
          {
            model: Vendor,
            required: true,
          },
          {
            model: VendorCase,
            required: true,
          },
        ],
      },
    ],
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

function calculateBillableAmount(
  amount: number | string,
  gainshareRate: number | string
): string {
  const amt = parseFloat(String(amount ?? '0'));
  const rate = parseFloat(String(gainshareRate ?? '0'));
  return (amt * rate).toFixed(4);
}

function hasDuplicateInvoiceAmounts(deductions: any[]): boolean {
  const invoiceAmounts = new Set<string>();
  for (const deduction of deductions) {
    const invoiceAmount = String(deduction.InvoiceAmount ?? '');
    if (invoiceAmounts.has(invoiceAmount)) {
      return true; // duplicate invoice amount
    }
    invoiceAmounts.add(invoiceAmount);
  }
  return false;
}

async function processPayments(
  payments: any[], // List of payments
  deductionMap: Map<string, any[]>, // Map of deductions by <key, deductions>
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

    // Skip if no deductions found
    if (deductionsForKey.length === 0) continue;

    // Pre-validation: Check if any deductions share the same invoice amount
    if (hasDuplicateInvoiceAmounts(deductionsForKey)) {
      console.log(
        `Skipping key ${key} - multiple deductions found with same invoice amount. Requires manual validation.`
      );
      continue;
    }

    for (const deduction of deductionsForKey) {
      if (deduction.InvoiceDate > payment.InvoiceDate) continue; // Skip if deduction date is after payment date

      const mapped = await mapPaymentToDeduction(payment, deduction, sequelize);
      if (mapped) break; // Only map one deduction per payment
    }
  }
}

async function mapPaymentToDeduction(
  payment: any,
  deduction: any,
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

  const auditManual = deduction.AuditManuals?.[0];
  if (!auditManual) {
    console.log(`Deduction ${deduction.Id} has no AuditManual`);
    return false;
  }

  const vendorCase = auditManual.VendorCase;
  const vendor = auditManual.Vendor;

  if (!vendorCase || !vendor || vendorCase.IsValidCase == null) return false;

  // Amount match
  // Only match if payment amount exactly equals the absolute value of the deduction amount
  if (
    parseFloat(String(payment.InvoiceAmount ?? '0')) !==
    Math.abs(parseFloat(String(deduction.InvoiceAmount ?? '0')))
  )
    return false;

  try {
    await sequelize.transaction(async (t: Transaction) => {
      console.log(
        '\n-----------------Initializing Transaction-----------------'
      );
      console.log(
        `Processing: Payment ${payment.Id} (${payment.InvoiceNumber}, ${payment.InvoiceAmount}) -> Deduction ${deduction.Id} (${deduction.InvoiceNumber}, ${deduction.InvoiceAmount})`
      );

      // Update deduction
      await RemittanceInvoices.update(
        { PaymentRemittanceId: payment.Id },
        { where: { Id: deduction.Id }, transaction: t }
      );

      // Check for existing billing ledger
      const existingBillingLedger = await BillingLedger.findOne({
        where: {
          VendorId: vendor.Id,
          OrganizationId: vendor.OrganizationId,
          BillingKey: String(payment.PaymentNumber ?? ''),
          BillingKey2: String(payment.InvoiceNumber ?? ''),
          KeySource: 'RemittanceInvoice',
        },
        transaction: t,
      });

      if (!existingBillingLedger) {
        // Calculate BillableAmount using vendor's GainshareRate
        const billableAmount = calculateBillableAmount(
          payment.InvoiceAmount,
          vendor.GainshareRate
        );

        const newLedger = await BillingLedger.create(
          {
            VendorId: vendor.Id,
            OrganizationId: vendor.OrganizationId,
            BillingKey: String(payment.PaymentNumber ?? ''),
            BillingKey2: String(payment.InvoiceNumber ?? ''),
            KeySource: 'RemittanceInvoice',
            Amount: String(payment.InvoiceAmount ?? ''),
            CurrencyCode: String(payment.InvoiceCurrency ?? ''),
            BillableAmount: billableAmount,
            BillableAmountCurrencyCode: payment.InvoiceCurrency,
            Description: `Billing for ${payment.InvoiceNumber}`,
            CaseId: vendorCase.Id?.toString(),
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
    }); // End of transaction

    console.log(
      `Successfully mapped payment ${payment.Id} to deduction ${deduction.Id}`
    );

    // Add a separator after each mapping/skip
    console.log('----------------------------------------------------------\n');

    mappedPaymentsCount++;
    return true;
  } catch (error) {
    console.error(
      `TRANSACTION ERROR: Error mapping payment ${payment.Id} to deduction ${deduction.Id}:`,
      error
    );
    console.log('----------------------------------------------------------\n');
    return false;
  }
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

    // Step 2: Build deduction map
    const deductionMap = buildDeductionMap(deductions);
    console.log('Deduction map keys:');

    // Step 3: Process payments mapping
    await processPayments(payments, deductionMap, sequelize);

    console.log('\nAuto-mapping complete.');
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
