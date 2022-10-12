const { createClient } = require("@supabase/supabase-js");

const supabaseURL = process.env.SUPABASE_URL;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

const supabase = createClient(supabaseURL, supabaseAnonKey);

module.exports = supabase;

/*
missing fields from properties table

ARV,
tax_code,
address,
grid,

missing fields from listing table */
