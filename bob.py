import asyncio
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, Message, ReplyKeyboardMarkup

BOT_TOKEN = "8646126772:AAGtZPWMPq-1WVP9POpceYgXGHATOduo6Ts"
ADMIN = [6777718761]  # пример: [123456789, 987654321]
MAIN_MENU_TEXT = "⬅️ Главное меню"


@dataclass
class Settings:
    bot_token: str
    admin_ids: set[int]


class Database:
    def __init__(self, path: Path = Path("bot.db")):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self):
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                balance REAL NOT NULL DEFAULT 0,
                referrer_id INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'available',
                taken_by INTEGER,
                taken_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                crypto_check TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                processed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                admin_reply TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL,
                replied_at TEXT
            );

            CREATE TABLE IF NOT EXISTS stats_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                photo_file_id TEXT NOT NULL,
                caption TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS action_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                action TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat(timespec="seconds")

    def upsert_user(self, user_id: int, username: str | None, first_name: str | None, referrer_id: int | None = None):
        cur = self.conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE users SET username=?, first_name=? WHERE user_id=?", (username, first_name, user_id))
        else:
            cur.execute(
                "INSERT INTO users(user_id, username, first_name, referrer_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, username, first_name, referrer_id, self._now()),
            )
            if referrer_id and referrer_id != user_id:
                cur.execute("UPDATE users SET balance = balance + 0.1 WHERE user_id=?", (referrer_id,))
        self.conn.commit()

    def log_action(self, user_id: int, username: str | None, action: str, details: str | None = None):
        self.conn.execute(
            "INSERT INTO action_logs(user_id, username, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, action, details, self._now()),
        )
        self.conn.commit()

    def add_link(self, url: str):
        self.conn.execute("INSERT INTO links(url, created_at) VALUES (?, ?)", (url, self._now()))
        self.conn.commit()

    def take_link(self, user_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT id, url FROM links WHERE status='available' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("UPDATE links SET status='taken', taken_by=?, taken_at=? WHERE id=?", (user_id, self._now(), row["id"]))
        self.conn.commit()
        return row

class Database:
            if referrer_id and referrer_id != user_id:
                cur.execute("UPDATE users SET balance = balance + 0.1 WHERE user_id=?", (referrer_id,))
        self.conn.commit()

    def log_action(self, user_id: int, username: str | None, action: str, details: str | None = None):
        self.conn.execute(
            "INSERT INTO action_logs(user_id, username, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, action, details, self._now()),
        )
        self.conn.commit()

    def add_link(self, url: str):
        self.conn.execute("INSERT INTO links(url, created_at) VALUES (?, ?)", (url, self._now()))
        self.conn.commit()

    def take_link(self, user_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT id, url FROM links WHERE status='available' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("UPDATE links SET status='taken', taken_by=?, taken_at=? WHERE id=?", (user_id, self._now(), row["id"]))
        self.conn.commit()
        return row

class Database:
            if referrer_id and referrer_id != user_id:
                cur.execute("UPDATE users SET balance = balance + 0.1 WHERE user_id=?", (referrer_id,))
        self.conn.commit()

    def log_action(self, user_id: int, username: str | None, action: str, details: str | None = None):
        self.conn.execute(
            "INSERT INTO action_logs(user_id, username, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, action, details, self._now()),
        )
        self.conn.commit()

    def add_link(self, url: str):
        self.conn.execute("INSERT INTO links(url, created_at) VALUES (?, ?)", (url, self._now()))
        self.conn.commit()

    def take_link(self, user_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT id, url FROM links WHERE status='available' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("UPDATE links SET status='taken', taken_by=?, taken_at=? WHERE id=?", (user_id, self._now(), row["id"]))
        self.conn.commit()
        return row

class Database:
            if referrer_id and referrer_id != user_id:
                cur.execute("UPDATE users SET balance = balance + 0.1 WHERE user_id=?", (referrer_id,))
        self.conn.commit()

    def log_action(self, user_id: int, username: str | None, action: str, details: str | None = None):
        self.conn.execute(
            "INSERT INTO action_logs(user_id, username, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, action, details, self._now()),
        )
        self.conn.commit()

    def add_link(self, url: str):
        self.conn.execute("INSERT INTO links(url, created_at) VALUES (?, ?)", (url, self._now()))
        self.conn.commit()

    def take_link(self, user_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT id, url FROM links WHERE status='available' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("UPDATE links SET status='taken', taken_by=?, taken_at=? WHERE id=?", (user_id, self._now(), row["id"]))
        self.conn.commit()
        return row


    def delete_all_links(self) -> int:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM links")
        self.conn.commit()
        return cur.rowcount

    def delete_links_by_ids(self, ids: list[int]) -> int:
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        cur = self.conn.cursor()
        cur.execute(f"DELETE FROM links WHERE id IN ({placeholders})", ids)
        self.conn.commit()
        return cur.rowcount

    def add_balance(self, user_id: int, amount: float) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        if not cur.fetchone():
            return False
        cur.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
        self.conn.commit()
        return True
     

    def recent_taken_links(self, limit: int = 20):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT l.id, l.url, l.taken_by, u.username
            FROM links l
            LEFT JOIN users u ON u.user_id=l.taken_by
            WHERE l.status='taken'
            ORDER BY l.id DESC LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()

    def get_balance(self, user_id: int) -> float:
        cur = self.conn.cursor()
        cur.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return float(row["balance"]) if row else 0.0

    def create_withdrawal(self, user_id: int, amount: float):
        self.conn.execute("INSERT INTO withdrawals(user_id, amount, created_at) VALUES (?, ?, ?)", (user_id, amount, self._now()))
        self.conn.commit()

    def pending_withdrawals(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT w.id, w.user_id, w.amount, u.username
            FROM withdrawals w
            LEFT JOIN users u ON u.user_id=w.user_id
            WHERE w.status='pending'
            ORDER BY w.id
            """
        )
        return cur.fetchall()

    def get_pending_withdrawal(self, withdrawal_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT w.id, w.user_id, w.amount, u.username
            FROM withdrawals w
            LEFT JOIN users u ON u.user_id=w.user_id
            WHERE w.status='pending' AND w.id=?
            """,
            (withdrawal_id,),
        )
        return cur.fetchone()

    def complete_withdrawal(self, withdrawal_id: int, check: str):
        cur = self.conn.cursor()
        cur.execute("SELECT user_id FROM withdrawals WHERE id=?", (withdrawal_id,))
        row = cur.fetchone()
        if not row:
            return None
        cur.execute(
            "UPDATE withdrawals SET status='done', crypto_check=?, processed_at=? WHERE id=?",
            (check, self._now(), withdrawal_id),
        )
        self.conn.commit()
        return row["user_id"]

    def add_support_ticket(self, user_id: int, message: str):
        self.conn.execute(
            "INSERT INTO support_tickets(user_id, message, created_at) VALUES (?, ?, ?)",
            (user_id, message, self._now()),
        )
        self.conn.commit()

    def open_tickets(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT t.id, t.user_id, t.message, u.username
            FROM support_tickets t
            LEFT JOIN users u ON u.user_id=t.user_id
            WHERE t.status='open'
            ORDER BY t.id
            """
        )
        return cur.fetchall()

    def answer_ticket(self, ticket_id: int, answer: str):
        cur = self.conn.cursor()
        cur.execute("SELECT user_id FROM support_tickets WHERE id=? AND status='open'", (ticket_id,))
        row = cur.fetchone()
        if not row:
            return None
        cur.execute(
            "UPDATE support_tickets SET admin_reply=?, status='closed', replied_at=? WHERE id=?",
            (answer, self._now(), ticket_id),
        )
        self.conn.commit()
        return row["user_id"]

    def add_stats_photo(self, admin_id: int, file_id: str, caption: str | None):
        self.conn.execute(
            "INSERT INTO stats_posts(admin_id, photo_file_id, caption, created_at) VALUES (?, ?, ?, ?)",
            (admin_id, file_id, caption, self._now()),
        )
        self.conn.commit()

    def last_stats_photo(self):
        cur = self.conn.cursor()
        cur.execute("SELECT photo_file_id, caption, created_at FROM stats_posts ORDER BY id DESC LIMIT 1")
        return cur.fetchone()

    def user_actions(self, user_id: int, limit: int = 30):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT action, details, created_at FROM action_logs WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        )
        return cur.fetchall()


def load_settings() -> Settings:
    if not BOT_TOKEN:
        raise RuntimeError("Заполните BOT_TOKEN в коде")
    admin_ids = {int(x) for x in ADMIN}
    return Settings(bot_token=BOT_TOKEN, admin_ids=admin_ids)


def user_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔗 Взять ссылку")],
            [KeyboardButton(text="💸 Вывод"), KeyboardButton(text="👥 Реферал")],
            [KeyboardButton(text="📊 Стата"), KeyboardButton(text="⚠️ Служба поддержки ⚠️")],
            [KeyboardButton(text="🔐 Админ-панель")],
        ],
        resize_keyboard=True,
    )


def admin_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить ссылку"), KeyboardButton(text="📦 Взятые ссылки")],
            [KeyboardButton(text="🖼 Загрузить стату"), KeyboardButton(text="✅ Обработать вывод")],
            [KeyboardButton(text="➕ Пополнить баланс"), KeyboardButton(text="🧹 Чистка ссылок")],
            [KeyboardButton(text="💬 Ответить поддержке"), KeyboardButton(text="🧾 Логи пользователя")],
        ],
        resize_keyboard=True,
    )


class AdminStates(StatesGroup):
    add_link = State()
    upload_stats = State()
    process_withdrawal = State()
    answer_support = State()
    view_logs = State()
    clean_links_specific = State()
    topup_balance = State()


class UserStates(StatesGroup):
    withdraw_amount = State()
    support_message = State()


settings = load_settings()
db = Database()
dp = Dispatcher()


def is_admin(user_id: int) -> bool:
    return user_id in settings.admin_ids


def pending_withdrawals_kb(rows) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                text=f"#{r['id']} | @{r['username'] or '-'} | {r['amount']}$",
                callback_data=f"withdraw_pick:{r['id']}",
            )
        ]
        for r in rows
    ]
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def withdrawal_action_kb(withdrawal_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💸 Выплатить", callback_data=f"withdraw_pay:{withdrawal_id}")],
            [InlineKeyboardButton(text="⬅️ К заявкам", callback_data="back:withdrawals")],
        ]
    )


def back_inline_kb(target: str = "main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back:{target}")]]
    )


@dp.message(CommandStart())
async def start(message: Message):
    referrer_id = None
    if message.text and len(message.text.split()) > 1:
        arg = message.text.split()[1].strip()
        if arg.isdigit():
            referrer_id = int(arg)
    db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name, referrer_id)
    db.log_action(message.from_user.id, message.from_user.username, "start", message.text)
    await message.answer("Бот запущен. Выберите действие:", reply_markup=user_kb())


@dp.message(F.text == "🔗 Взять ссылку")
async def take_link(message: Message):
    db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    link = db.take_link(message.from_user.id)
    db.log_action(message.from_user.id, message.from_user.username, "take_link")
    if not link:
        await message.answer("Пока нет доступных ссылок.")
        return
    await message.answer(f"Ваша ссылка #{link['id']}: {link['url']}")


@dp.message(F.text == "📊 Стата")
async def stats(message: Message):
    item = db.last_stats_photo()
    db.log_action(message.from_user.id, message.from_user.username, "stats")
    if not item:
        await message.answer("Стата пока не загружена админом.")
        return
    await message.answer_photo(item["photo_file_id"], caption=item["caption"] or f"Обновлено: {item['created_at']}")


@dp.message(F.text == "👥 Реферал")
async def referral(message: Message, bot: Bot):
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start={message.from_user.id}"
    db.log_action(message.from_user.id, message.from_user.username, "referral")
    await message.answer(
        "За каждого реферала начисляется 0.1$.\n"
        "Реферал засчитывается при первом старте бота.\n"
        f"Ваша реферальная ссылка:\n{link}"
    )


@dp.message(F.text == "💸 Вывод")
async def withdraw_start(message: Message, state: FSMContext):
    balance = db.get_balance(message.from_user.id)
    db.log_action(message.from_user.id, message.from_user.username, "withdraw_open", f"balance={balance}")
    await state.set_state(UserStates.withdraw_amount)
    await message.answer(
        f"Вывод только в Crypto Bot. Ваш баланс: {balance:.2f}$\nВведите сумму для вывода:",
        reply_markup=back_inline_kb("main"),
    )


@dp.message(UserStates.withdraw_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if text in {
        "🔗 Взять ссылку",
        "👥 Реферал",
        "📊 Стата",
        "⚠️ Служба поддержки ⚠️",
        "🔐 Админ-панель",
        MAIN_MENU_TEXT,
    }:
        await state.clear()
        if text == "🔗 Взять ссылку":
            await take_link(message)
            return
        if text == "👥 Реферал":
            await referral(message, message.bot)
            return
        if text == "📊 Стата":
            await stats(message)
            return
        if text == "⚠️ Служба поддержки ⚠️":
            await support_start(message, state)
            return
        if text == "🔐 Админ-панель":
            await open_admin_panel(message)
            return
        await open_main_menu(message, state)
        return

    try:
        amount = float(text.replace(",", "."))
    except Exception:
        await message.answer("Введите сумму числом или выберите любую кнопку меню.")
        return
    balance = db.get_balance(message.from_user.id)
    if amount <= 0 or amount > balance:
        await message.answer(f"Недостаточно баланса. Доступно: {balance:.2f}$")
        return
    db.create_withdrawal(message.from_user.id, amount)
    db.log_action(message.from_user.id, message.from_user.username, "withdraw_request", f"amount={amount}")
    await state.clear()
    await message.answer("Заявка на вывод создана. Ожидайте подтверждения администратора.")


@dp.message(F.text == "⚠️ Служба поддержки ⚠️")
async def support_start(message: Message, state: FSMContext):
    db.log_action(message.from_user.id, message.from_user.username, "support_open")
    await state.set_state(UserStates.support_message)
    await message.answer(
        "Возникла проблема или вопрос? Напишите сюда одним сообщением.",
        reply_markup=back_inline_kb("main"),
    )


@dp.message(UserStates.support_message)
async def support_send(message: Message, state: FSMContext):
    db.add_support_ticket(message.from_user.id, message.text)
    db.log_action(message.from_user.id, message.from_user.username, "support_create", message.text)
    await state.clear()
    await message.answer("Ваш запрос в поддержку отправлен.")


@dp.message(F.text == "🔐 Админ-панель")
async def open_admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет доступа к админ-панели.")
        return
    await message.answer("Админ-панель открыта.", reply_markup=admin_kb())


@dp.message(StateFilter("*"), F.text == MAIN_MENU_TEXT)
async def open_main_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=user_kb())


@dp.message(F.text == "➕ Добавить ссылку")
async def admin_add_link_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.add_link)
    await message.answer("Пришлите ссылку для загрузки.")


@dp.message(AdminStates.add_link)
async def admin_add_link(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.add_link(message.text.strip())
    db.log_action(message.from_user.id, message.from_user.username, "admin_add_link", message.text)
    await state.clear()
    await message.answer("Ссылка добавлена.")


@dp.message(F.text == "📦 Взятые ссылки")
async def admin_taken_links(message: Message):
    if not is_admin(message.from_user.id):
        return
    rows = db.recent_taken_links()
    if not rows:
        await message.answer("Пока никто не взял ссылки.")
        return
    lines = ["Последние взятые ссылки:"]
    for idx, row in enumerate(rows, start=1):
        lines.append(f"{idx}. #{row['id']} {row['url']} | @{row['username'] or '-'} | id:{row['taken_by']}")
    await message.answer("\n".join(lines))


@dp.message(F.text == "🖼 Загрузить стату")
async def admin_upload_stats_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.upload_stats)
    await message.answer("Пришлите фото статистики с подписью (необязательно).")


@dp.message(AdminStates.upload_stats, F.photo)
async def admin_upload_stats(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.add_stats_photo(message.from_user.id, message.photo[-1].file_id, message.caption)
    db.log_action(message.from_user.id, message.from_user.username, "admin_upload_stats")
    await state.clear()
    await message.answer("Стата обновлена.")


@dp.message(F.text == "✅ Обработать вывод")
async def admin_process_withdraw(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    rows = db.pending_withdrawals()
    if not rows:
        await message.answer("Нет заявок на вывод.")
        return
    await message.answer("Заявки на вывод (нажмите на пользователя):", reply_markup=pending_withdrawals_kb(rows))


@dp.callback_query(F.data.startswith("withdraw_pick:"))
async def withdraw_pick(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    withdrawal_id = int(callback.data.split(":", 1)[1])
    row = db.get_pending_withdrawal(withdrawal_id)
    if not row:
        await callback.answer("Заявка уже обработана")
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
        return

    await callback.answer()
    if callback.message:
        await callback.message.answer(
            f"Заявка #{row['id']}\n"
            f"Пользователь: @{row['username'] or '-'} (id:{row['user_id']})\n"
            f"Сумма: {row['amount']}$",
            reply_markup=withdrawal_action_kb(withdrawal_id),
        )


@dp.callback_query(F.data.startswith("withdraw_pay:"))
async def withdraw_pay_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    withdrawal_id = int(callback.data.split(":", 1)[1])
    row = db.get_pending_withdrawal(withdrawal_id)
    if not row:
        await callback.answer("Заявка уже обработана")
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
        return

    await state.set_state(AdminStates.process_withdrawal)
    await state.update_data(withdrawal_id=withdrawal_id)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            f"Заявка #{row['id']} выбрана.\n"
            "Отправьте одним сообщением ссылку/сообщение для выплаты.",
            reply_markup=back_inline_kb("withdrawals"),
        )


@dp.message(AdminStates.process_withdrawal)
async def admin_complete_withdraw(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    withdrawal_id = data.get("withdrawal_id")
    if not withdrawal_id:
        await state.clear()
        await message.answer("Сначала выберите заявку через кнопку '✅ Обработать вывод'.")
        return

    check = (message.text or "").strip()
    if not check:
        await message.answer("Отправьте текстом ссылку/сообщение для выплаты.")
        return

    user_id = db.complete_withdrawal(int(withdrawal_id), check)
    if not user_id:
        await state.clear()
        await message.answer("Заявка не найдена или уже обработана.")
        return

    db.log_action(message.from_user.id, message.from_user.username, "admin_complete_withdrawal", f"id={withdrawal_id} check={check}")
    await bot.send_message(user_id, f"✅ Выплата обработана.\n{check}")
    await state.clear()
    await message.answer("Вывод отмечен как выполненный.")

    rows = db.pending_withdrawals()
    if rows:
        await message.answer("Оставшиеся заявки:", reply_markup=pending_withdrawals_kb(rows))
    else:
        await message.answer("Все заявки обработаны, список пуст.")


@dp.message(F.text == "🧹 Чистка ссылок")
async def admin_links_cleanup_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("Выберите режим чистки ссылок:", reply_markup=links_cleanup_kb())


@dp.callback_query(F.data == "links_clean:all")
async def admin_links_cleanup_all(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    deleted = db.delete_all_links()
    db.log_action(callback.from_user.id, callback.from_user.username, "admin_clean_links_all", f"deleted={deleted}")
    await callback.answer("Готово")
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(f"Удалено ссылок: {deleted}.", reply_markup=admin_kb())


@dp.callback_query(F.data == "links_clean:specific")
async def admin_links_cleanup_specific_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.clean_links_specific)
    await callback.answer()
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("Отправьте ID ссылок через запятую (например: 1,2,15).")


@dp.message(AdminStates.clean_links_specific)
async def admin_links_cleanup_specific_apply(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").replace(" ", "")
    parts = [x for x in raw.split(",") if x]
    if not parts or any(not x.isdigit() for x in parts):
        await message.answer("Неверный формат. Пример: 1,2,15")
        return
    ids = [int(x) for x in parts]
    deleted = db.delete_links_by_ids(ids)
    db.log_action(message.from_user.id, message.from_user.username, "admin_clean_links_specific", f"ids={ids} deleted={deleted}")
    await state.clear()
    await message.answer(f"Удалено ссылок: {deleted}.", reply_markup=admin_kb())


@dp.message(F.text == "➕ Пополнить баланс")
async def admin_topup_balance_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.topup_balance)
    await message.answer("Введите в формате user_id|сумма (например: 123456|10.5)")


@dp.message(AdminStates.topup_balance)
async def admin_topup_balance_apply(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if "|" not in text:
        await message.answer("Формат: user_id|сумма")
        return
    left, right = text.split("|", 1)
    left, right = left.strip(), right.strip().replace(",", ".")
    if not left.isdigit():
        await message.answer("user_id должен быть числом")
        return
    try:
        amount = float(right)
    except ValueError:
        await message.answer("Сумма должна быть числом")
        return
    if amount <= 0:
        await message.answer("Сумма должна быть больше 0")
        return

    user_id = int(left)
    ok = db.add_balance(user_id, amount)
    if not ok:
        await message.answer("Пользователь не найден. Пусть сначала запустит бота через /start")
        return

    db.log_action(message.from_user.id, message.from_user.username, "admin_topup_balance", f"user_id={user_id} amount={amount}")
    await state.clear()
    await message.answer(f"Баланс пользователя {user_id} пополнен на {amount:.2f}$", reply_markup=admin_kb())


@dp.message(StateFilter("*"), F.text == "🔐 Админ-панель")
async def open_admin_panel_any_state(message: Message, state: FSMContext):
    await state.clear()
    await open_admin_panel(message)


@dp.callback_query(F.data.startswith("back:"))
async def inline_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    target = callback.data.split(":", 1)[1]
    await callback.answer()
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=None)

    if target == "admin":
        if not is_admin(callback.from_user.id):
            if callback.message:
                await callback.message.answer("Нет доступа к админ-панели.", reply_markup=user_kb())
            return
        if callback.message:
            await callback.message.answer("Админ-панель открыта.", reply_markup=admin_kb())
        return

    if target == "withdrawals":
        if not is_admin(callback.from_user.id):
            if callback.message:
                await callback.message.answer("Нет доступа к выплатам.", reply_markup=user_kb())
            return
        rows = db.pending_withdrawals()
        if callback.message:
            if rows:
                await callback.message.answer("Заявки на вывод (нажмите на пользователя):", reply_markup=pending_withdrawals_kb(rows))
            else:
                await callback.message.answer("Нет заявок на вывод.", reply_markup=admin_kb())
        return

    if callback.message:
        await callback.message.answer("Главное меню:", reply_markup=user_kb())


@dp.message(F.text == "💬 Ответить поддержке")
async def admin_support_open(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    rows = db.open_tickets()
    if not rows:
        await message.answer("Открытых вопросов нет.")
        return
    lines = ["Открытые вопросы (формат ответа: id|текст):"]
    for r in rows:
        lines.append(f"{r['id']} | @{r['username'] or '-'} | user:{r['user_id']} | {r['message']}")
    await state.set_state(AdminStates.answer_support)
    await message.answer("\n".join(lines))


@dp.message(AdminStates.answer_support)
async def admin_support_answer(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    if "|" not in message.text:
        await message.answer("Формат: id|текст")
        return
    left, answer = message.text.split("|", 1)
    if not left.strip().isdigit():
        await message.answer("ID должен быть числом")
        return
    user_id = db.answer_ticket(int(left.strip()), answer.strip())
    if not user_id:
        await message.answer("Вопрос не найден или уже закрыт.")
        return
    db.log_action(message.from_user.id, message.from_user.username, "admin_answer_support", message.text)
    await bot.send_message(user_id, f"Ответ поддержки:\n{answer.strip()}")
    await state.clear()
    await message.answer("Ответ отправлен.")


@dp.message(F.text == "🧾 Логи пользователя")
async def admin_logs_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.view_logs)
    await message.answer("Введите user_id для просмотра действий.")


@dp.message(AdminStates.view_logs)
async def admin_logs_show(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if not message.text.strip().isdigit():
        await message.answer("Нужен числовой user_id")
        return
    rows = db.user_actions(int(message.text.strip()))
    if not rows:
        await message.answer("Логи пустые.")
    else:
        lines = ["Последние действия пользователя:"]
        for r in rows:
            lines.append(f"{r['created_at']} | {r['action']} | {r['details'] or '-'}")
        await message.answer("\n".join(lines))
    await state.clear()


async def main():
    bot = Bot(settings.bot_token)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
