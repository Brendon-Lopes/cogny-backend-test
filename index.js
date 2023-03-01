const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

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

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    const fetchData = async () => {
        const { data: { data } } = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');
        return data;
    }

    const insertData = async (data) => {
        await Promise.all(data.map((item) => {
            db[DATABASE_SCHEMA].api_data.insert({
                doc_record: item
            });
        }));
        console.log('data inserted to database')
    }

    const sumPopulationJs = (data, fromYear, toYear) => {
        return data.reduce((acc, { doc_record }) => {
            const year = Number(doc_record.Year);
            const population = doc_record.Population

            if (year >= fromYear && year <= toYear) acc += population;

            return acc;
        }, 0);
    }

    try {
        await migrationUp();

        // adiciona os dados da api no banco de dados
        await insertData(await fetchData());

        // busca os dados do banco de dados
        const data = await db[DATABASE_SCHEMA].api_data.find({
            is_active: true
        });

        // soma as populações usando apenas JS
        const sumJavascript = sumPopulationJs(data, 2018, 2020);

        // cria uma view da soma das populações
        await db.query(`
            CREATE VIEW ${DATABASE_SCHEMA}.soma_populacao AS
            SELECT SUM((doc_record ->> 'Population')::INTEGER) AS soma_populacao
            FROM ${DATABASE_SCHEMA}.api_data
            WHERE (doc_record ->> 'Year')::INTEGER BETWEEN 2018 AND 2020;
        `);

        // busca a soma das populações pela view criada
        const [sumView] = await db.query(
            `SELECT * FROM ${DATABASE_SCHEMA}.soma_populacao;`
        );

        const result = [
            { method: 'SQL Query', populationSum: Number(sumView.soma_populacao) },
            { method: 'JavaScriptl', populationSum: sumJavascript }
        ]

        console.table(result);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();
