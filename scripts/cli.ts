/// <reference path="../typings/index.d.ts" />

import * as firebase from 'firebase';
import * as _ from 'lodash';

var log = {
  info: console.log,
  debug: console.log,
  trace: console.log,
  setDefaultLevel: (x) => {}
}; // couldn't get loglevel to work, so faking it this way

require('dotenv').config();

log.info(`starting with NODE_ENV ${process.env.NODE_ENV} and FIREBASE_PROJECT_ID ${process.env.FIREBASE_PROJECT_ID}`);

firebase.initializeApp({
  serviceAccount: `./serviceAccountCredentials.${process.env.FIREBASE_PROJECT_ID}.json`,
  databaseURL: `https://${process.env.FIREBASE_PROJECT_ID}.firebaseio.com`
});

let usersRef = () => {
  return firebase.database().ref(`/users`);
};

let userRef = (id) => {
  return usersRef().child(id);
};

let users;


usersRef().once('value', (snapshot) => {
  users = snapshot.val() || {};
  _.each(users, (u,uid) => { u.userId = uid; });
  log.info(`${_.size(users)} users loaded`);
});

usersRef().once('value', (snapshot) => {
  users = snapshot.val() || {};
  _.each(users, (u,uid) => { u.userId = uid; });
  log.info(`${_.size(users)} users loaded`);

  let recordsProcessed = 0;
  let recordsUpdated = 0;

  let setNewDownlineLevel = (s) => {
    recordsProcessed++;
    let newDownlineLevel = s.downlineLevel + 1;
    _.each(s.downlineUsers,(_,duid) => {
      let d = users[duid];
      if (!d) {
        log.info(`this shouldn't happen! for duid ${duid}`);
        return;
      }
      if (d.downlineLevel !== newDownlineLevel) {
        d.downlineLevel = newDownlineLevel;
        userRef(duid).child('downlineLevel').set(newDownlineLevel);
        recordsUpdated++;
      }
      setNewDownlineLevel(d);
    });
  };

  let startTime = new Date();
  let topUser = _.find(users, (u) => { return u.email === 'jreitano@ur.technology' });
  setNewDownlineLevel(topUser);
  let endTime = new Date();
  let minutesElapsed = (endTime - startTime)/1000/60;
  log.info(`minutesElapsed=`, minutesElapsed);
  log.info(`processed records=`,recordsProcessed);
  log.info(`updated records=`,recordsUpdated);
});

setNewDownlineUsers = (users, u, index) => {
  let directReferrals = _.pick(users, (d) => { return d.sponsor && d.sponsor.userId === u.userId});
  let newDownlineUsers = _.mapValues(directReferrals, (d) => { return  _.pick(d, ['name', 'profilePhotoUrl']); });
  if (u.downlineUsers && _.isEmpty(newDownlineUsers)) {
    u.newDownlineUsers = newDownlineUsers;
    userRef(u.userId).child('downlineUsers').remove();
  } else if (!_.isEmpty(newDownlineUsers) && !_.isEqual(u.downlineUsers, newDownlineUsers)) {
    u.newDownlineUsers = newDownlineUsers;
    userRef(u.userId).child('downlineUsers').set(newDownlineUsers);
  }
};

let users;
usersRef().once('value', (snapshot) => {
  let users = snapshot.val() || {};
  _.each(users, (u,uid) => { u.userId = uid; });
  log.info(`${_.size(users)} users loaded`);

  let startTime = new Date();
  let index = 0;
  _.each(users, (u) => {
    setNewDownlineLevel(users, u, index);
    index++;
  });
  let endTime = new Date();
  let minutesElapsed = (endTime - startTime)/1000/60;
  log.info(`minutesElapsed=`, minutesElapsed);
  log.info(`updated records=`,_.size(_.filter(users, 'newDownlineLevel')));
});

let vipEmails = ['k.fingal@gmail.com', 'bugmenot134@yahoo.com', 'mikepola3@gmail.com', 'barbarahagood1@gmail.com', 'courtneydanne@gmail.com', 'schroder.jordanb@gmail.com', 'grahamj412@gmail.com', 'jenniferlovesjamie@comcast.net', 'viviana.schaller@outlook.de', 'moh.rokib.94.mr@gmail.com', 'lulasmom617@gmail.com', 'santiagoenriqueescobedo@gmail.com', 'nimblues@gmail.com', 'mamadmomod238@gmail.com', 'tripp@chamblin.com', 'nacnaus2@gmail.com', 'mcrawford@rymanhp.com', 'adam@sobrolaw.com', 'stever@equitas-intl.com', 'wtwebb2@comcast.net', 'clement.webb@gmail.com', 'holybloood89@gmail.ua', 'flynn.mcrae21@montgomerybell.edu', 'herofrog243@gmail.com', 'mcdugaldj@gmail.com', 'max.d.giorgio@gmail.com', 'wrenkis@me.com', 'truettmckeehan@gmail.com', 'howardeglover@gmail.com', 'btcman007@gmail.com', 'slimane.bouaoud1@gmail.com', 'jannejac@gmail.com', 'thomasmcdaniel9@gmail.com', 'karin.kristiansson62@gmail.com', 'mats.betterglobe@gmail.com', 'ggtupe@yahoo.com', 'ternovicnews@gmail.com', 'vladeo1x@gmail.com', 'iqball26@hotmail.com'];
let vips = _.map(vipEmails, (e) => { return _.find(users, (u) => { return u.email === e; }) });
_.map(vips, (u) => { return `${u.email}\t${ ( u.registration && u.registration.status ) || 'created-wallet'}`; });

let groups = _.groupBy(vips, (u) => {
  if (!u.wallet || !u.wallet.address) {
    return "never-logged-in";
  } else {
    return ( u.registration && u.registration.status ) || 'created-wallet';
  }
});
let groups2 = _.mapValues(groups, (g, status) => { let m = {}; _.each(g, (u) => { m[u.email] = u; }); return m; });
