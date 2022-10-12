require("dotenv").config();
const fs = require("fs");
const path = require("path");
const csv = require("fast-csv");
const http = require("https");
const { phone } = require("phone");
const { v4: uuidv4 } = require("uuid");
const supabase = require("./supabaseClient");

// YYYY-MM-DD_skipper_properties.csv;

const getFileName = () => {
  const date = new Date("2022-10-9");
  const day = date.toLocaleString("en-US", { day: "numeric" });
  const month = date.toLocaleString("en-US", { month: "2-digit" });
  const year = date.toLocaleString("en-US", { year: "numeric" });
  const fileName = `${year}-${month}-${day}_skipper_properties.csv`;
  return fileName;
};

const getPublicURL = () => {
  const fileName = getFileName();
  const { publicURL, error } = supabase.storage
    .from("csv")
    .getPublicUrl(fileName);
  console.log(publicURL);
  return { publicURL, error };
};

const downloadFile = (fileURL) => {
  const fileName = getFileName();
  const file = fs.createWriteStream(`./data/${fileName}`);
  const request = http.get(fileURL, function (response) {
    response.pipe(file);
    file.on("finish", async () => {
      file.close();
      console.log("downloaded successfully");
    });
  });
};

// const { publicURL, error } = getPublicURL();
// if (error) {
//   console.log("Error");
// } else {
//   downloadFile(publicURL);
// }

const setManualReviewObject = (row, supabaseImages) => {
  const manualReviewObject = {
    properties_skipper_id: row.skipper_id ? Number(row.skipper_id) : null,
    assessment_number: row.assessment_number
      ? Number(row.assessment_number)
      : null,
    name: row.name,
    city: row.city,
    zip: row.zip ? Number(row.zip) : null,
    flag: row.flag,
    longitude: row.longitude,
    latitude: row.latitude,
    property_type: row.property_type,
    url: row.url,
    views: row.views,
    special_headline: row.special_headline,
    short_description: row.short_description,
    long_description: row.long_description,
    youtube_id: row.youtube_id,
    vimeo_id: row.vimeo_id,
    rego_embed_id: row.rego_embed_id,
    paradym_url: row.paradym_url,
    virtual_tour_url: row.virtual_tour_url,
    virtual_tour_img: row.virtual_tour_img,
    images: supabaseImages,
    bedrooms: row.bedrooms ? Number(row.bedrooms) : null,
    bathrooms: row.bathrooms ? Number(row.bathrooms) : null,
    half_bathrooms: row.half_bathrooms ? Number(row.half_bathrooms) : null,
    lotsize: row.lotsize ? Number(row.lotsize) : null,
    sqft: row.sqft ? Number(row.sqft) : null,
    arv: null, // missing
    tax_code: null, //missing
    address: null, //missing
    grid: null, // missing
  };
  return manualReviewObject;
};

const setListingsObject = (row) => {
  const listingsObject = {
    date_added: row.date_added ? new Date(row.date_added) : null,
    date_relisted: row.date_relisted ? new Date(row.date_relisted) : null,
    is_rent: row.is_rent ? Number(row.is_rent) : null,
    is_sale: row.is_sale ? Number(row.is_sale) : null,
    under_contract: row.under_contract ? Number(row.under_contract) : null,
    under_offer: row.under_offer ? Number(row.under_offer) : null,
    buyer_type: row.buyer_type,
    price: row.price ? Number(row.price) : null,
    price_from: row.price_from ? Number(row.price_from) : null,
    daily_rate: row.daily_rate ? Number(row.daily_rate) : null,
    is_let: row.is_let ? Number(row.is_let) : null,
    properties_skipper_id: row.skipper_id ? Number(row.skipper_id) : null,
  };
  return listingsObject;
};

const setAgentsObject = (agentArray, agent_listing_id) => {
  let agentEmail = null;
  agentEmail = agentArray.find((item) => item.search("@") >= 0);
  const agentObject = {
    agent_number: agentArray[0] ? Number(agentArray[0]) : null,
    agent_name: agentArray[1] ? agentArray[1].trim() : null,
    company: agentArray[2] ? agentArray[2].trim() : null,
    email: agentEmail,
    phone: agentArray[4] ? agentArray[4].trim() : null,
    agent_listing_id,
  };
  return agentObject;
};
const insertToManualReviewAgents = (agentsData) => {
  return new Promise(async (resolve, reject) => {
    try {
      const { data, error } = await supabase
        .from("manual_review_agents")
        .insert([agentsData]);
      if (error) throw error;
      else {
        resolve(data);
      }
    } catch (error) {
      console.log(error);
      reject("Intert into manual_reivew_agent failed");
    }
  });
};

const insertToManualReviewListings = (listingsData) => {
  return new Promise(async (resolve, reject) => {
    try {
      const { data, error } = await supabase
        .from("manual_review_listings")
        .insert([listingsData]);
      if (error) throw error;
      else {
        resolve(data);
      }
    } catch (error) {
      console.log(error);
      reject("Intert into manual_reivew_listing failed");
    }
  });
};

const insertToManualReview = (manualReviewData) => {
  return new Promise(async (resolve, reject) => {
    try {
      const { data, error } = await supabase
        .from("manual_review")
        .insert([manualReviewData]);
      if (error) throw error;
      else {
        resolve(data);
      }
    } catch (error) {
      console.log(error);
      reject("Intert into manual reivew failed");
    }
  });
};

const convertStringToArray = (str, substr) => {
  let result = [];
  let newStr = str.slice(1, -1);
  let resultString = newStr;

  let idx = newStr.indexOf(substr);

  while (idx !== -1) {
    result.push(idx - 1);
    idx = newStr.indexOf(substr, idx + 1);
  }
  let indx = 0;
  for (var i = 0; i < result.length; i++) {
    if (newStr.charAt(result[i]) === "b") {
      resultString =
        resultString.slice(0, result[i] - indx) +
        resultString.slice(result[i] + 1 - indx);
      indx = indx + 1;
    }
  }

  return resultString.replaceAll("'", "").split(",");
};

const downloadImages = (images) => {
  return new Promise((resolve, reject) => {
    let newArr = [];

    for (var i = 0; i < images.length; i++) {
      const fileName = `${uuidv4()}-image.jpg`;
      const filePath = `./images/${fileName}`;
      const file = fs.createWriteStream(filePath);
      const request = http.get(images[i], function (response) {
        response.pipe(file);
        file.on("finish", async () => {
          file.close();
          newArr.push(filePath);
          if (newArr.length === images.length) {
            resolve(newArr);
          }
        });
      });
    }
  });
};

const uploadImagesToSupabase = (images) => {
  return new Promise(async (resolve, reject) => {
    let newArr = [];
    for (var i = 0; i < images.length; i++) {
      try {
        const img = fs.readFileSync(images[i]);
        const fileName = images[i].slice(9);
        const { error } = await supabase.storage
          .from("kw-images")
          .upload(`${fileName}`, img, {
            contentType: "image/jpg",
          });
        if (error) throw error;
        else {
          const { publicURL, error } = supabase.storage
            .from("kw-images")
            .getPublicUrl(`${fileName}`);
          if (error) throw error;
          else {
            newArr.push(publicURL);
          }
        }
      } catch (error) {
        console.log(error);
      }
    }
    resolve(newArr);
  });
};

const readFromCSV = () => {
  let data = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(
      path.resolve(__dirname, "data", "2022-10-9_skipper_properties.csv")
    )
      .pipe(csv.parse({ headers: true }))
      .on("error", (error) => {
        console.log(error);
        reject("Failed to read for the CSV file");
      })
      .on("data", async (row) => {
        data.push(row);
      })
      .on("end", (rowCount) => {
        console.log(`Parsed ${rowCount} rows`);
        resolve(data);
      });
  });
};

(async () => {
  try {
    const res = await readFromCSV();
    for (var i = 0; i < res.length; i++) {
      try {
        if (!res[i].assessment_number) {
          console.log("manual_review_processing");
          const imagesArray = convertStringToArray(res[i].images, "'");
          const localImages = await downloadImages(imagesArray);
          let supabaseImagesArray = await uploadImagesToSupabase(localImages);
          const manualReviewObject = setManualReviewObject(
            res[i],
            supabaseImagesArray
          );
          const manualReviewResponse = await insertToManualReview(
            manualReviewObject
          );
          console.log("manual_review_insert_success");
        }

        console.log("manual_review_listing_processing");
        const listingsObject = setListingsObject(res[i]);
        const manualReviewListingsRespone = await insertToManualReviewListings(
          listingsObject
        );
        console.log("manual_review_listing_insert_success");
        if (manualReviewListingsRespone) {
          if (res[i].agent) {
            console.log("manual_review_agent_processing");
            const agentArray = convertStringToArray(res[i].agent, "'");
            const agentsObject = setAgentsObject(
              agentArray,
              manualReviewListingsRespone[0].listing_id
            );
            const manualReviewAgentsResponse = await insertToManualReviewAgents(
              agentsObject
            );
            console.log("manual_review_agent_insert_success");
          }
        }
      } catch (error) {
        console.log(error);
      }
    }
  } catch (error) {
    console.log(error);
  }
})();

// readFromCSV()
//   .then((res) => (csvData = res))
//   .catch((error) => console.log(error));

// fs.createReadStream(
//   path.resolve(__dirname, "data", "2022-10-9_skipper_properties.csv")
// )
//   .pipe(csv.parse({ headers: true }))
//   .on("error", (error) => console.error(error))
//   .on("data", async (row) => {
// let supabaseImagesArray;
// const imagesArray = convertStringToArray(row.images, "'");
// const localImages = await downloadImages(row);
// supabaseImagesArray = await uploadImagesToSupabase(localImages);
// try {
//   if (!row.assessment_number) {
//     const manualReviewObject = setManualReviewObject(
//       row,
//       supabaseImagesArray
//     );
//     const manualReviewResponse = await insertToManualReview(
//       manualReviewObject
//     );
//     console.log(manualReviewResponse);
//   }
//   const listingsObject = setListingsObject(row);
//   const manualReviewListingsRespone = await insertToManualReviewListings(
//     listingsObject
//   );
//   console.log(manualReviewListingsRespone);
// } catch (error) {
//   console.log(error);
// }
//   })
//   .on("end", (rowCount) => console.log(`Parsed ${rowCount} rows`));
