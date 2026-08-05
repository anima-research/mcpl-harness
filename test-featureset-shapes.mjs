// Issue #4 regression: both featureSets wire shapes register without crashing.
import { HostSession } from './dist/src/session.js';

const calls = [];
const fake = Object.create(HostSession.prototype);
fake.featureSets = {};
fake.store = { registerFeatureSet: (name, opts) => calls.push([name, opts]) };
fake.emit = () => {};

const reg = (caps) => Object.getPrototypeOf(fake).constructor.prototype
  ? HostSession.prototype['registerDeclaredFeatureSets'].call(fake, caps)
  : null;

// Conforming 0.5 object shape (previously: "(decls ?? []) is not iterable")
reg({ experimental: { mcpl: { featureSets: {
  'demo.messaging': { description: 'd', uses: ['pushEvents'], rollback: true },
  'demo.plain': { description: 'p', uses: ['tools'] },
} } } });
if (!fake.featureSets['demo.messaging']) { console.error('object shape not registered'); process.exit(1); }
if (calls.length !== 1 || calls[0][0] !== 'demo.messaging') { console.error('rollback registration wrong', calls); process.exit(1); }

// Legacy 0.4 array shape still works
fake.featureSets = {}; calls.length = 0;
reg({ experimental: { mcpl: { featureSets: [
  { name: 'legacy.set', rollback: false, hostState: true },
] } } });
if (!fake.featureSets['legacy.set']) { console.error('array shape not registered'); process.exit(1); }
if (calls.length !== 1 || calls[0][1].hostState !== true) { console.error('legacy hostState wrong', calls); process.exit(1); }

// Absent / junk shapes are inert, never a throw
reg({}); reg({ experimental: { mcpl: { featureSets: 42 } } }); reg(null);

console.log('# pass 3 (object shape, array shape, junk-inert)');
