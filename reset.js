const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');

// Call start
(async () => {
  console.log('main.js: before start');

  const db = await massive({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  }, {
    // Massive Configuration
    scripts: process.cwd() + '/migration',
    allowedSchemas: [DATABASE_SCHEMA],
    whitelist: [`${DATABASE_SCHEMA}.%`],
    excludeFunctions: true,
  }, {
    // Driver Configuration
    noWarnings: true,
    error: function (err, client) {
      console.log(err);
      //process.emit('uncaughtException', err);
      //throw err;
    }
  });

  if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
    monitor.attach(db.driverConfig);
  }

  try {
    await db[DATABASE_SCHEMA].api_data.destroy({})

    console.log('database reset')

    const data = await db[DATABASE_SCHEMA].api_data.find({
      is_active: true
    });

    await db.query(`DROP VIEW IF EXISTS ${DATABASE_SCHEMA}.soma_populacao;`)

    console.log({ data });

  } catch (e) {
    console.log(e.message)
  } finally {
    console.log('finally');
  }
  console.log('main.js: after start');
})();