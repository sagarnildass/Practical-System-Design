import time
import threading
from snowflake_id_generator import SnowflakeIDGenerator

class UserService:
    """Simulates a user service that creates user records with unique IDs."""
    
    def __init__(self, datacenter_id, machine_id):
        self.id_generator = SnowflakeIDGenerator(datacenter_id, machine_id)
        self.users = {}
        self.lock = threading.Lock()
    
    def create_user(self, name, email):
        """Create a new user with a unique ID.
        
        Args:
            name (str): User's name
            email (str): User's email
            
        Returns:
            dict: The created user object
        """
        # Generate a unique ID
        user_id = self.id_generator.next_id()
        
        # Create user object
        created_at = time.time()
        user = {
            "id": user_id,
            "name": name,
            "email": email,
            "created_at": created_at
        }
        
        # Store the user
        with self.lock:
            self.users[user_id] = user
        
        return user
    
    def get_user(self, user_id):
        """Get a user by ID.
        
        Args:
            user_id (int): The user's ID
            
        Returns:
            dict: The user object, or None if not found
        """
        return self.users.get(user_id)
    
    def list_users(self):
        """List all users, sorted by ID (which is also sorted by creation time).
        
        Returns:
            list: List of user objects
        """
        with self.lock:
            return [user for _, user in sorted(self.users.items())]

class PostService:
    """Simulates a post service that creates post records with unique IDs."""
    
    def __init__(self, datacenter_id, machine_id):
        self.id_generator = SnowflakeIDGenerator(datacenter_id, machine_id)
        self.posts = {}
        self.lock = threading.Lock()
    
    def create_post(self, user_id, content):
        """Create a new post with a unique ID.
        
        Args:
            user_id (int): ID of the user creating the post
            content (str): Post content
            
        Returns:
            dict: The created post object
        """
        # Generate a unique ID
        post_id = self.id_generator.next_id()
        
        # Create post object
        created_at = time.time()
        post = {
            "id": post_id,
            "user_id": user_id,
            "content": content,
            "created_at": created_at
        }
        
        # Store the post
        with self.lock:
            self.posts[post_id] = post
        
        return post
    
    def get_post(self, post_id):
        """Get a post by ID.
        
        Args:
            post_id (int): The post's ID
            
        Returns:
            dict: The post object, or None if not found
        """
        return self.posts.get(post_id)
    
    def list_posts_by_user(self, user_id):
        """List all posts by a specific user, sorted by ID (creation time).
        
        Args:
            user_id (int): The user's ID
            
        Returns:
            list: List of post objects
        """
        with self.lock:
            user_posts = [post for post in self.posts.values() if post["user_id"] == user_id]
            return sorted(user_posts, key=lambda x: x["id"])

def run_simulation():
    """Run a simulation of the user and post services."""
    print("Starting social media application simulation...")
    
    # Create services
    # In a real distributed system, these might be on different servers
    user_service = UserService(datacenter_id=1, machine_id=1)
    post_service = PostService(datacenter_id=1, machine_id=2)
    
    # Create some users
    alice = user_service.create_user("Alice", "alice@example.com")
    bob = user_service.create_user("Bob", "bob@example.com")
    charlie = user_service.create_user("Charlie", "charlie@example.com")
    
    print(f"\nCreated users:")
    for user in user_service.list_users():
        parsed_id = SnowflakeIDGenerator.parse_id(user["id"])
        print(f"- {user['name']} (ID: {user['id']}, Created: {parsed_id['generated_time']})")
    
    # Let Bob create some posts
    time.sleep(0.1)  # Small delay to make timestamps more realistic
    post1 = post_service.create_post(bob["id"], "Hello, world!")
    
    time.sleep(0.2)
    post2 = post_service.create_post(bob["id"], "I love distributed systems!")
    
    time.sleep(0.3)
    post3 = post_service.create_post(bob["id"], "Unique IDs are fascinating!")
    
    # Let Alice create a post
    time.sleep(0.1)
    post4 = post_service.create_post(alice["id"], "Hi everyone, I'm new here!")
    
    print(f"\nCreated posts:")
    all_posts = [post1, post2, post3, post4]
    for post in all_posts:
        user = user_service.get_user(post["user_id"])
        parsed_id = SnowflakeIDGenerator.parse_id(post["id"])
        print(f"- {user['name']}: \"{post['content']}\" (ID: {post['id']}, Created: {parsed_id['generated_time']})")
    
    # Demonstrate time-sorted IDs
    print("\nAll posts sorted by ID (which is also sorted by time):")
    sorted_posts = sorted(all_posts, key=lambda x: x["id"])
    for post in sorted_posts:
        user = user_service.get_user(post["user_id"])
        parsed_id = SnowflakeIDGenerator.parse_id(post["id"])
        print(f"- {user['name']}: \"{post['content']}\" (ID: {post['id']}, Created: {parsed_id['generated_time']})")
    
    # Demonstrate the relationship between IDs and timestamps
    first_id = sorted_posts[0]["id"]
    last_id = sorted_posts[-1]["id"]
    
    first_time = SnowflakeIDGenerator.parse_id(first_id)["generated_time"]
    last_time = SnowflakeIDGenerator.parse_id(last_id)["generated_time"]
    
    print(f"\nFirst ID: {first_id}, generated at {first_time}")
    print(f"Last ID: {last_id}, generated at {last_time}")
    print(f"IDs are guaranteed to be time-sortable: {first_id < last_id}")

if __name__ == "__main__":
    run_simulation() 