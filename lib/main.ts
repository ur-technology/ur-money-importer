/// <reference path="../typings/index.d.ts" />

import * as dotenv from 'dotenv';
import * as log from 'loglevel';
import * as _ from 'lodash';
import * as firebase from 'firebase';
import {DataManipulator} from './data_manipulator';

dotenv.config();
log.setDefaultLevel(process.env.LOG_LEVEL || "info")
log.info(`starting with NODE_ENV ${process.env.NODE_ENV} and FIREBASE_PROJECT_ID ${process.env.FIREBASE_PROJECT_ID}`);

let serviceAccount = require(`../serviceAccountCredentials.${process.env.FIREBASE_PROJECT_ID}.json`);
let admin = require("firebase-admin");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: `https://${process.env.FIREBASE_PROJECT_ID}.firebaseio.com`
});

let dataManipulator = new DataManipulator(process.env, admin.database(), admin.auth());

dataManipulator.disableFraudulentUser('xxx@example.com');

dataManipulator.displayStats();

dataManipulator.checkTransactions();

// dataManipulator.unblockUsers();
// dataManipulator.openMissingFreshdeskTickets();
// dataManipulator.announceUsers();

// log.info("all done!");

process.on('SIGTERM', () => {
  log.info(`Exiting...`);
  process.exit(0);
});
