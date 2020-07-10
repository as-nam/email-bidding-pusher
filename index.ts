import fs from "fs";
import axios from "axios";
import moment from "moment";
import readline from "readline";

const url =
    "https://api.emailbidding.com/api/p/publishers/172/lists/de_eb_as/recipients-batch?key=5d07251f58ae4e2cc6578d6333ede432&secret=77b9fa8e8211b4613619a015c81b984c8e52e220";

let labels = [];
let recipients = [];
let total = 0;

function mapArrayToObject(array) {
    const object = {};
    array.forEach((value, index) => {
        object[labels[index]] = value;
    });
    return object;
}

function formatDatetime(datetime) {
    try {
        if (datetime && moment(datetime, "YYYY-MM-DD").isValid()) {
            return datetime ? moment(datetime).format("YYYY-MM-DD") : "";
        }
        return "";
    } catch (e) {
        return "";
    }
}

function formatGender(gender) {
    if (gender === "M" || gender === "F") {
        return gender;
    }
    return "NA";
}

function formatZipCode(zip) {
    zip = zip.split(".")[0];
    if (zip && zip.length <= 10) {
        return zip;
    }
    return "";
}

function parsingRecipients(recipients) {
    const array = [];
    recipients.forEach((recipient) => {
        array.push({
            email_address: recipient.email,
            country: "DE",
            first_name: recipient.first_name || "",
            last_name: recipient.last_name || "",
            external_id: recipient.galaxy_id,
            gender: formatGender(recipient.gender),
            zipcode: formatZipCode(recipient.zip),
            birthdate: formatDatetime(recipient.dob),
        });
    });
    return array;
}

(async () => {
    const fileStream = fs.createReadStream("20200701_compiled_main_csa_2m.csv");
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
    });
    let i = 0;
    for await (const line of rl) {
        if (i === 0) {
            labels = line.split(",");
        } else {
            recipients.push(mapArrayToObject(line.split(",")));
        }

        if (i === 200) {
            const rows = parsingRecipients(recipients);
            const data = {
                subscribe: {
                    recipients: rows,
                },
            };
            try {
                const response = await axios({
                    url,
                    method: "POST",
                    data,
                });
                fs.writeFileSync(
                    `reports/success_${total}.json`,
                    JSON.stringify(response.data),
                    "utf8"
                );
                total += 200;
                console.log(total);
            } catch (e) {
                if (e.response && e.response.status === 400) {
                    fs.writeFileSync(
                        `reports/error_${total}.json`,
                        JSON.stringify(
                            e.response.data.errors.children.recipients
                        ),
                        "utf8"
                    );
                } else {
                    console.log(e);
                    fs.writeFileSync(
                        `reports/failed_${total}.json`,
                        JSON.stringify(e),
                        "utf8"
                    );
                }
            }

            i = 1;
            recipients = [];
        }
        i++;
    }
    const rows = parsingRecipients(recipients);
    const response = await axios({
        url,
        method: "POST",
        data: {
            subscribe: {
                recipients: rows,
            },
        },
    });
    console.log(total + recipients.length);
    fs.writeFileSync(
        `reports/success_${total}.json`,
        JSON.stringify(response.data),
        "utf8"
    );
})();
