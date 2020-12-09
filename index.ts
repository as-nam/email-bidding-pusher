import fs from 'fs';
import axios from 'axios';
import moment from 'moment';
import readline from 'readline';
import { Recipient } from './recipient.type';

const LIST = {
  DE_AS: 'DE_EB_AS',
  DE_MC: 'DE_MC',
  UK: 'UK_1220',
  FR_MC: 'FR_MC'
};
const url = `https://api.emailbidding.com/api/p/publishers/170/lists/fr_mc/recipients-batch?key=ca318982a21f29ce6777ac28df564109&secret=df762df7ce7709fc0e61c895fd2e2fca4fcd1d89`;

let labels = [];
let recipients: Recipient[] = [];
let total = 0;

function mapArrayToObject(array: string[]): Recipient {
  const object = {};
  array.forEach((value, index) => {
    object[labels[index]] = value;
  });

  return object as Recipient;
}

function formatRecipientList() {
  recipients.forEach((recipient) => {
    recipient.country = recipient.country;
    recipient.gender = formatGender(recipient.gender);
    recipient.last_activity_date = formatLastActivityDate(
      recipient.last_activity_date
    );
    recipient.birthdate = formatBirthDate(recipient.birthdate);
  });
}

function formatBirthDate(timestamp: string): string {
  try {
    if (timestamp) {
      if (moment(new Date(timestamp)).isValid()) {
        if (moment(new Date(timestamp)).isValid()) {
          return moment(timestamp).format('YYYY-MM-DD');
        }
      }
      return '';
    }
    return '';
  } catch (e) {
    return '';
  }
}

function formatLastActivityDate(timestamp: string): string {
  try {
    if (timestamp) {
      if (moment(new Date(timestamp)).isValid()) {
        return moment(timestamp).format('YYYY-MM-DD HH:mm:ss');
      }
      return '';
    }
    return '';
  } catch (e) {
    return '';
  }
}

function formatGender(gender: string): string {
  if (gender === 'M' || gender === 'F') {
    return gender;
  }
  return 'NA';
}

const pushRecipientToEmailBidding = async () => {
  try {
    const response = await axios({
      url,
      method: 'POST',
      data: {
        subscribe: {
          recipients
        }
      }
    });
    fs.writeFileSync(
      `reports/${LIST.FR_MC}/success_${total}.json`,
      JSON.stringify(response.data),
      'utf8'
    );
  } catch (e) {
    if (e.response && e.response.status === 400) {
      fs.writeFileSync(
        `reports/${LIST.FR_MC}/error_${total}.json`,
        JSON.stringify(e.response.data.errors.children.recipients),
        'utf8'
      );
    } else {
      fs.writeFileSync(
        `reports/${LIST.FR_MC}/failed_${total}.json`,
        JSON.stringify(e),
        'utf8'
      );
    }
  }
  total += recipients.length;
  console.log(total);
};

let isHeader = false;

(async () => {

  console.log(url);


  // if (!fs.existsSync(`reports/${LIST.DE_AS}`)) {
  //   fs.mkdirSync(`reports/${LIST.DE_AS}`);
  // }

  // if (!fs.existsSync(`reports/${LIST.DE_MC}`)) {
  //   fs.mkdirSync(`reports/${LIST.FR_MC}`);
  // }

  // if (!fs.existsSync(`reports/${LIST.UK}`)) {
  //   fs.mkdirSync(`reports/${LIST.UK}`);
  // }

  // if (!fs.existsSync(`./reports/${LIST.FR_MC}`)) {
  //   fs.mkdirSync(`reports/${LIST.FR_MC}`);
  // }


  const fileStream = fs.createReadStream(
    './DE_UK_FR_Emailbidding/UK_Emailbidding_963k.csv'
  );
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  let i = 0;
  for await (const line of rl) {
    if (!isHeader) {
      labels = line.split(',');
      isHeader = true;
    } else {
      formatRecipientList();
      recipients.push(mapArrayToObject(line.split(',')));
      i++;
    }

    if (i === 200) {
      i = 0;
      await pushRecipientToEmailBidding();
      recipients = [];
    }
  }
  await pushRecipientToEmailBidding();
})();
