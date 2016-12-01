let users;
users = undefined;
usersRef().once('value', (snapshot) => {
  users = snapshot.val() || {};
  _.each(users, (u,uid) => { u.userId = uid; });
  log.info(`${_.size(users)} users loaded`);
});

g = _.groupBy(users, (u,uid) => { return u.registration && u.registration.status; });
_.mapValues(g, (a,status) => { return _.size(a);});


needAnnouncement = _.filter(users, (u,uid) => {
  if (!u.registration || u.registration.status !== 'verification-succeeded') {
    return false;
  }
  let sponsor = u.sponsor && u.sponsor.userId && users[u.sponsor.userId];
  return sponsor && sponsor.status === 'announcement-confirmed';
});
_.size(needAnnouncement);


_.each(users, (u,uid) => {
  if (u.registration && u.registration.status === 'announcement-succeeded') {
    userRef(uid).child('registration').update({status: 'announcement-confirmed'});
  }
});
_.size(needAnnouncment);


usersRef().orderByChild('registration/status').equalTo('verification-pending').once('value').then((snapshot) => {
  log.info(_.size(snapshot.val()||{}));
});

let users;
users = {};
usersRef().once('value', (snapshot) => {
  users = snapshot.val() || {};
  log.info(`${_.size(users)} users loaded`);

  let numNeedFixing = 0;
  let numFixed = 0;
  _.each(users, (u,uid) => {
    if (u.downlineLevel !== undefined) {
      return;
    }
    numNeedFixing++;
    let sponsor = u.sponsor && u.sponsor.userId && users[u.sponsor.userId];
    let sponsorDownlineLevel = sponsor && sponsor.downlineLevel;
    if (sponsorDownlineLevel) {
      userRef(uid).update({downlineLevel: sponsor.downlineLevel + 1});
      numFixed++;
    }
  });
  log.info(`${numNeedFixing} needed fixing`);
  log.info(`${numFixed} fixed`);
});


let emailAuthCodeGenerationTasks;
let emailAuthCodeMatchingTasks;

emailAuthCodeGenerationTasks = {};
firebase.database().ref('/emailAuthCodeGenerationQueue/tasks').once('value', (snapshot) => {
  emailAuthCodeGenerationTasks = snapshot.val() || {};
  log.info(`${_.size(emailAuthCodeGenerationTasks)} emailAuthCodeGenerationTasks loaded`);

  emailAuthCodeGenerationTasks.groups = _.groupBy(emailAuthCodeGenerationTasks, (o,oid) => { return o._state; });
  emailAuthCodeGenerationTasks.result = _.mapValues(emailAuthCodeGenerationTasks.groups, (a,status) => { return _.size(a);});
});

emailAuthCodeMatchingTasks = {};
firebase.database().ref('/emailAuthCodeMatchingQueue/tasks').once('value', (snapshot) => {
  emailAuthCodeMatchingTasks = snapshot.val() || {};
  log.info(`${_.size(emailAuthCodeMatchingTasks)} emailAuthCodeMatchingTasks loaded`);

  emailAuthCodeMatchingTasks.groups = _.groupBy(emailAuthCodeMatchingTasks, (o,oid) => { return o._state; });
  emailAuthCodeMatchingTasks.result = _.mapValues(emailAuthCodeMatchingTasks.groups, (a,status) => { return _.size(a);});
});

directReferrals = (u) => { return _.pick(users, (d) => { return d.sponsor && d.sponsor.userId === u.userId}) };
topUser = _.find(users, (u) => { return u.email === 'jreitano@ur.technology' });
_.each(users, (u) => {
  delete u.downlineUsers;
});
_.each(users, (u) => {
  if (u.downlineUsers === undefined) {
    u.downlineUsers = _.pick(directReferrals(u), ['name', 'profilePhotoUrl']);
    if (_.size(u.downlineUsers) > 50) {
      log.info(`big user ${u.email}`);
    }
  }
});

_.each(users, (u) => { delete u.downlineSize; });
downlineSize = (u) => {
  if (!_.isNumber(u.downlineSize)) {
    let fullDirectReferrals = _.map(u.downlineUsers, (d,did) => { return users[did]; });
    u.downlineSize = _.size(u.downlineUsers) + _.sum(fullDirectReferrals, downlineSize);
  }
  return u.downlineSize;
};
topUser = _.find(users, (u) => { return u.email === 'jreitano@ur.technology' });
downlineSize(topUser);


 _.each(users, (u) => { delete u.downlineSize; });
 downlineSize = (u) => {
   let directReferrals = _.pick(users, (d) => { return d.sponsor && d.sponsor.userId === u.userId});
   u.processed = true;
   return _.size(directReferrals) + _.sum(directReferrals, (d) => { return downlineSize(d); })
 };

downlineSize = (u) => {
  let directReferrals = _.pick(users, (d) => { return d.sponsor && d.sponsor.userId === u.userId});
  u.processed = true;
  return _.size(directReferrals) + _.sum(directReferrals, (d) => { return downlineSize(d); })
};
downlineSize(topUser);
