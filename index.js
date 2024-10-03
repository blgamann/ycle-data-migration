const { Client } = require("pg");
require("dotenv").config();

const sourceClient = new Client({
  connectionString: process.env.SOURCE_DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

const destClient = new Client({
  connectionString: process.env.DEST_DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

const userIdMap = new Map();
const cycleIdMap = new Map();

async function migrateData() {
  try {
    await sourceClient.connect();
    await destClient.connect();

    await destClient.query("BEGIN");

    await migrateUsers();
    await migrateCycles();
    await updateRecycledFromId();
    await migrateLikes();
    await migrateComments();

    await destClient.query("COMMIT");

    console.log("데이터 마이그레이션이 성공적으로 완료되었습니다.");
  } catch (error) {
    console.log("error", error);
    await destClient.query("ROLLBACK");
    console.error("데이터 마이그레이션 중 오류 발생:", error);
  } finally {
    await sourceClient.end();
    await destClient.end();
  }
}

async function migrateUsers() {
  const res = await sourceClient.query("SELECT * FROM public.users");

  for (const row of res.rows) {
    const insertRes = await destClient.query(
      `INSERT INTO public."User" (username, password, why, mediums, "createdAt")
       VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
       RETURNING id`,
      [row.username, row.password, row.why, row.medium]
    );

    userIdMap.set(row.id, insertRes.rows[0].id);
  }

  console.log(`사용자 데이터 마이그레이션 완료: ${userIdMap.size}명`);
}

async function migrateCycles() {
  const res = await sourceClient.query("SELECT * FROM public.cycles");

  for (const row of res.rows) {
    const newUserId = userIdMap.get(row.user_id);

    if (!newUserId) {
      console.warn(`사용자 ID 매핑을 찾을 수 없습니다: ${row.user_id}`);
      continue;
    }

    const recycledFromOldId = row.recycled_from;
    const recycledFromNewId = recycledFromOldId
      ? cycleIdMap.get(recycledFromOldId)
      : null;

    const insertRes = await destClient.query(
      `INSERT INTO public."Cycle" ("userId", reflection, medium, "imageUrl", "createdAt",
        "eventDescription", "eventDate", "eventStartTime", "eventEndTime", "eventLocation", "recycledFromId")
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING id`,
      [
        newUserId,
        row.reflection || "",
        row.medium,
        row.img_url,
        row.created_at,
        row.event_description,
        row.event_date,
        row.event_start_time ? row.event_start_time.toString() : null,
        row.event_end_time ? row.event_end_time.toString() : null,
        row.event_location,
        recycledFromNewId,
      ]
    );

    cycleIdMap.set(row.id, insertRes.rows[0].id);
  }

  console.log(`사이클 데이터 마이그레이션 완료: ${cycleIdMap.size}개`);
}

async function updateRecycledFromId() {
  const res = await sourceClient.query(
    "SELECT id, recycled_from FROM public.cycles WHERE recycled_from IS NOT NULL"
  );

  for (const row of res.rows) {
    const oldCycleId = row.id;
    const recycledFromOldId = row.recycled_from;
    const newCycleId = cycleIdMap.get(oldCycleId);
    const recycledFromNewId = cycleIdMap.get(recycledFromOldId);

    if (!newCycleId || !recycledFromNewId) {
      console.warn(
        `사이클 ID 매핑을 찾을 수 없습니다: cycle_id=${oldCycleId}, recycled_from_id=${recycledFromOldId}`
      );
      continue;
    }

    await destClient.query(
      `UPDATE public."Cycle" SET "recycledFromId" = $1 WHERE id = $2`,
      [recycledFromNewId, newCycleId]
    );
  }

  console.log("recycledFromId 필드 업데이트 완료.");
}

async function migrateLikes() {
  const res = await sourceClient.query("SELECT * FROM public.likes");

  for (const row of res.rows) {
    const newUserId = userIdMap.get(row.user_id);
    const newCycleId = cycleIdMap.get(row.cycle_id);

    if (!newUserId || !newCycleId) {
      console.warn(
        `좋아요 데이터 매핑을 찾을 수 없습니다: user_id=${row.user_id}, cycle_id=${row.cycle_id}`
      );
      continue;
    }

    await destClient.query(
      `INSERT INTO public."Like" ("userId", "cycleId", "createdAt")
       VALUES ($1, $2, $3)`,
      [newUserId, newCycleId, row.created_at]
    );
  }

  console.log(`좋아요 데이터 마이그레이션 완료: ${res.rowCount}개`);
}

async function migrateComments() {
  const res = await sourceClient.query("SELECT * FROM public.comments");

  for (const row of res.rows) {
    const newUserId = userIdMap.get(row.user_id);
    const newCycleId = cycleIdMap.get(row.cycle_id);

    if (!newUserId || !newCycleId) {
      console.warn(
        `댓글 데이터 매핑을 찾을 수 없습니다: user_id=${row.user_id}, cycle_id=${row.cycle_id}`
      );
      continue;
    }

    await destClient.query(
      `INSERT INTO public."Comment" (content, "userId", "cycleId", "createdAt")
       VALUES ($1, $2, $3, $4)`,
      [row.content, newUserId, newCycleId, row.created_at]
    );
  }

  console.log(`댓글 데이터 마이그레이션 완료: ${res.rowCount}개`);
}

migrateData();
